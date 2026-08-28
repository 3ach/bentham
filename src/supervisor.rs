//! Session lifecycle: one resumable `claude -p` chain per channel. The
//! dispatcher watches the buffer and scrub queue; each wake runs one turn,
//! carrying a fresh session token that scopes every tool call.

use crate::buffer::WakeTarget;
use crate::state::{ScrubJob, Shared, TurnCtx};
use crate::{consent, persona, prompts};
use serde_json::Value;
use std::collections::HashSet;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::process::Command;
use tokio::time::{Duration, Instant, sleep, sleep_until, timeout};

#[derive(Clone, PartialEq)]
enum WakeKind {
    Activity,
    Idle,
    /// Non-conversational persona scrub after an opt-out.
    Scrub(ScrubJob),
}

pub async fn run(shared: Arc<Shared>) {
    let mcp_cfg = serde_json::json!({
        "mcpServers": {
            "discord": { "type": "http", "url": format!("http://127.0.0.1:{}/mcp", shared.cfg.mcp.port) }
        }
    });
    if let Err(e) = std::fs::write(shared.mcp_config_path(), mcp_cfg.to_string()) {
        tracing::error!("writing mcp.json: {e}");
        return;
    }
    if let Some(map) = std::fs::read_to_string(shared.state_path())
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
    {
        shared.sessions.load(map);
    }

    let busy: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let mut last_active: Option<WakeTarget> = None;
    let mut idle_deadline = next_idle_deadline(&shared, &last_active).await;

    loop {
        // Register for wakeups before checking, so nothing slips between.
        let notified = shared.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // Persona scrubs first; they borrow the consent post's channel slot.
        let jobs: Vec<ScrubJob> = shared.sessions.take_scrubs();
        let mut requeue = Vec::new();
        for job in jobs {
            let Some(chan) = consent::post_of(&shared, &job.scope.to_string()).await.map(|p| p.channel_id)
            else {
                continue;
            };
            if busy.lock().unwrap().insert(chan.clone()) {
                let target = WakeTarget {
                    channel_id: chan,
                    name: format!("maintenance ({})", job.scope),
                    is_dm: false,
                    scope: job.scope,
                };
                spawn_turn(&shared, &busy, target, WakeKind::Scrub(job));
            } else {
                requeue.push(job);
            }
        }
        if !requeue.is_empty() {
            shared.sessions.requeue_scrubs(requeue);
        }

        let ready: Vec<WakeTarget> = {
            let behaviors = shared.behaviors.read().await.clone();
            let busy_now = busy.lock().unwrap().clone();
            shared
                .buffer
                .wakeworthy(&behaviors)
                .into_iter()
                .filter(|t| !busy_now.contains(&t.channel_id))
                .collect()
        };

        if ready.is_empty() {
            tokio::select! {
                _ = notified => {}
                _ = sleep_until(idle_deadline) => {
                    if let Some(t) = last_active.clone()
                        && persona::behavior_for(&shared, t.scope).await.idle_wake_minutes > 0
                        && busy.lock().unwrap().insert(t.channel_id.clone())
                    {
                        spawn_turn(&shared, &busy, t, WakeKind::Idle);
                    }
                    idle_deadline = next_idle_deadline(&shared, &last_active).await;
                }
            }
            continue;
        }

        for t in ready {
            last_active = Some(t.clone());
            busy.lock().unwrap().insert(t.channel_id.clone());
            spawn_turn(&shared, &busy, t, WakeKind::Activity);
        }
        idle_deadline = next_idle_deadline(&shared, &last_active).await;
    }
}

async fn next_idle_deadline(shared: &Arc<Shared>, last: &Option<WakeTarget>) -> Instant {
    let minutes = match last {
        Some(t) => persona::behavior_for(shared, t.scope).await.idle_wake_minutes,
        None => 0,
    };
    let secs = if minutes > 0 { minutes * 60 } else { 365 * 24 * 3600 };
    Instant::now() + Duration::from_secs(secs)
}

fn spawn_turn(shared: &Arc<Shared>, busy: &Arc<Mutex<HashSet<String>>>, target: WakeTarget, kind: WakeKind) {
    let (shared, busy) = (shared.clone(), busy.clone());
    tokio::spawn(async move {
        turn(&shared, &target, kind).await;
        busy.lock().unwrap().remove(&target.channel_id);
        // New activity may have arrived while we were finishing up.
        shared.notify.notify_waiters();
    });
}

/// One wake: build the prompt, run one `claude -p` turn, record the session.
async fn turn(shared: &Arc<Shared>, target: &WakeTarget, kind: WakeKind) {
    let cfg = &shared.cfg.claude;
    if kind == WakeKind::Activity {
        sleep(Duration::from_secs(cfg.debounce_seconds)).await;
    }

    let channel_id = &target.channel_id;
    let (st, first_time, gen_at_start) = shared.sessions.begin_turn(channel_id);
    let fresh = st.session_id.is_none() || st.wakes >= cfg.session_max_wakes;

    let system = prompts::system(&persona::read(shared, target.scope).await);
    let place = format!("{} (channel_id {channel_id})", target.name);
    let body = match &kind {
        WakeKind::Scrub(job) => prompts::scrub(&job.user_name, &job.user_id),
        _ if first_time && !target.is_dm => prompts::first_summon(&place),
        _ if fresh => prompts::fresh(&place),
        WakeKind::Idle => prompts::idle(&place),
        _ => prompts::activity(&place),
    };
    let token = shared.tokens.issue(TurnCtx {
        channel_id: channel_id.clone(),
        scope: target.scope,
        is_dm: target.is_dm,
        maintenance: matches!(kind, WakeKind::Scrub(_)),
    });
    let prompt = prompts::with_token(&body, &token);

    let mut cmd = Command::new(&cfg.binary);
    cmd.arg("-p")
        .arg(&prompt)
        .arg("--output-format")
        .arg("json")
        .arg("--model")
        .arg(&cfg.model)
        .arg("--mcp-config")
        .arg(shared.mcp_config_path())
        .arg("--strict-mcp-config")
        .arg("--allowed-tools")
        .arg("mcp__discord")
        .arg("--append-system-prompt")
        .arg(&system);
    if !cfg.disallowed_tools.is_empty() {
        cmd.arg("--disallowed-tools").args(&cfg.disallowed_tools);
    }
    if !fresh && let Some(id) = &st.session_id {
        cmd.arg("--resume").arg(id);
    }
    cmd.args(&cfg.extra_args);
    cmd.current_dir(&shared.cfg.data_dir)
        .env("MCP_TOOL_TIMEOUT", "600000")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    tracing::info!(channel = target.name, scope = %target.scope, fresh, first_time, "waking claude");
    shared.typing.turn_started(channel_id);
    let started = Instant::now();
    let outcome = run_claude(cmd, Duration::from_secs(cfg.turn_timeout_minutes * 60)).await;
    shared.typing.turn_ended(channel_id);
    shared.tokens.revoke(&token);

    let ok = match outcome {
        Err(e) => {
            tracing::error!(channel = target.name, "turn failed: {e}");
            false
        }
        Ok(res) => {
            let is_error = res["is_error"].as_bool().unwrap_or(false);
            tracing::info!(
                channel = target.name,
                is_error,
                turns = res["num_turns"].as_u64().unwrap_or(0),
                cost_usd = res["total_cost_usd"].as_f64().unwrap_or(0.0),
                secs = started.elapsed().as_secs(),
                "turn done: {}",
                res["result"].as_str().unwrap_or("").chars().take(300).collect::<String>()
            );
            // If a scrub invalidated memory mid-turn, this transcript dies with it.
            let (gen_ok, reapable) =
                shared.sessions.record_session(channel_id, res["session_id"].as_str(), fresh, gen_at_start);
            if gen_ok {
                shared.save_sessions();
            } else {
                tracing::info!(channel = target.name, "scrub happened mid-turn; not recording session");
            }
            // Either the id displaced by a fresh turn, or the unrecorded
            // post-scrub one; nothing references it, so it dies here.
            if let Some(old) = reapable {
                shared.reap_transcripts(&[old]);
            }
            !is_error
        }
    };

    if ok {
        shared.sessions.clear_failures(channel_id);
    } else {
        let (failures, dropped, old_id) = shared.sessions.record_failure(channel_id);
        if dropped {
            tracing::warn!(channel = target.name, "3 consecutive failures, dropping session");
        }
        shared.save_sessions();
        if let Some(old) = old_id {
            shared.reap_transcripts(&[old]);
        }
        // Back off while still holding this channel's busy slot: 4s, 8s,
        // then 2s after the reset (`2 << failures` from the original code).
        sleep(Duration::from_secs(2u64.saturating_pow(failures + 1).min(60))).await;
    }
}

/// Spawn claude, wait (killing on timeout via kill_on_drop), parse its result JSON.
async fn run_claude(mut cmd: Command, limit: Duration) -> Result<Value, String> {
    let child = cmd.spawn().map_err(|e| format!("spawn: {e}"))?;
    let out = timeout(limit, child.wait_with_output())
        .await
        .map_err(|_| "turn timed out (killed)".to_string())?
        .map_err(|e| format!("wait: {e}"))?;
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).map_err(|e| {
        let stderr: String = String::from_utf8_lossy(&out.stderr).chars().take(400).collect();
        format!("unparseable output ({e}); stderr: {stderr}")
    })
}
