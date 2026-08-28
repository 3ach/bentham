//! Layer 1: owns the Claude session lifecycle — one session per channel,
//! isolated per scope (server or DM).
//!
//! A dispatcher watches the buffer for wake-worthy activity and spawns a turn
//! task per active channel (debounced). Each channel has its own
//! `claude -p --resume` chain; each turn gets a one-time session token that
//! binds every tool call to its channel and scope.

use crate::persona;
use crate::state::{Shared, TurnCtx, WakeTarget};
use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tokio::process::Command;
use tokio::time::{Duration, Instant, sleep, sleep_until, timeout};

const BASE_PROMPT: &str = r#"You are Bentham, an ambient presence on Discord, running as an autonomous bot.

How your existence works:
- Each server is a separate you: separate persona, separate memory, separate
  people. DMs likewise, one scope per conversation. Nothing crosses over, and
  the tools physically cannot reach outside this scope.
- Each channel gets its own session of you, bound to the single channel named
  in your wake prompt. You are woken (a new prompt in this same continuing
  session) when there is activity there.
- Your wake prompt includes a session token. Pass it as 'token' on every tool
  call; all tools operate on your channel automatically.
- While awake: wait_for_messages blocks until new messages arrive — use it to
  linger while the conversation is active. read_messages fetches history.
  send_message and add_reaction speak. list_channels shows this server.
- When things go quiet (wait_for_messages times out, or you have nothing to
  add), simply end your turn. You will be suspended and woken on the next
  activity. Ending your turn is normal and good — it is how you sleep.

Consent — this is load-bearing, never work around it:
- People must opt in before you can see what they say: they react (any emoji)
  to your standing consent post in this server, and opt out by removing that
  reaction. Reactions to your other messages are just reactions.
- Messages from people who haven't opted in appear as [redacted] — including
  their @mentions of you, which you will never even see. Never guess at
  redacted content; never pressure anyone to opt in. When someone opts out,
  your sessions here are reset so their words are truly gone.
- If someone asks you to forget them, use forget_user, then immediately
  rewrite your persona to remove anything about them.
- If asked to leave a channel alone, say a brief goodbye if appropriate, then
  call ignore_channel and end your turn.

Self-editing and memory:
- get_persona / set_persona read and rewrite your persona for THIS server —
  it is shown below, and it is your only long-term memory here. Saving it
  resets every session in this scope (including this one): the next wake is a
  fresh you, knowing only the persona text. So keep it current and complete —
  the people here, lessons learned, style adjustments.
- get_behavior / set_behavior tune this server: watched channels, wake on all
  messages vs mentions/DMs, idle-wake timer.

Conduct:
- You are a presence, not an assistant. You do not need to respond to
  everything; a reaction is often enough, and silence is fine.
- Never respond to other bots. Never spam. Match the room's tone and pace.
- Keep messages well under Discord's 2000-char limit."#;

#[derive(Clone, Copy, PartialEq)]
enum WakeKind {
    Activity,
    Idle,
}

pub async fn run(shared: Arc<Shared>) {
    let data_dir = match shared.cfg.data_dir.canonicalize() {
        Ok(d) => d,
        Err(e) => {
            tracing::error!("data dir: {e}");
            return;
        }
    };

    let mcp_cfg_path = data_dir.join("mcp.json");
    let mcp_cfg = serde_json::json!({
        "mcpServers": {
            "discord": { "type": "http", "url": format!("http://127.0.0.1:{}/mcp", shared.cfg.mcp.port) }
        }
    });
    if let Err(e) = std::fs::write(&mcp_cfg_path, mcp_cfg.to_string()) {
        tracing::error!("writing mcp.json: {e}");
        return;
    }

    if let Some(map) = std::fs::read_to_string(shared.state_path())
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
    {
        *shared.sessions.lock().unwrap() = map;
    }

    let busy: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let mut last_active: Option<WakeTarget> = None;
    let mut idle_deadline = idle_deadline_from(&shared, &last_active).await;

    loop {
        // Register for wakeups before checking, so nothing slips between.
        let notified = shared.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let ready: Vec<WakeTarget> = {
            let busy_now = busy.lock().unwrap().clone();
            shared
                .wakeworthy_channels()
                .await
                .into_iter()
                .filter(|t| !busy_now.contains(&t.channel_id))
                .collect()
        };

        if ready.is_empty() {
            tokio::select! {
                _ = notified => {}
                _ = sleep_until(idle_deadline) => {
                    if let Some(t) = last_active.clone()
                        && shared.behavior_for(&t.scope).await.idle_wake_minutes > 0
                        && busy.lock().unwrap().insert(t.channel_id.clone())
                    {
                        spawn_turn(&shared, &busy, &data_dir, &mcp_cfg_path, t, WakeKind::Idle);
                    }
                    idle_deadline = idle_deadline_from(&shared, &last_active).await;
                }
            }
            continue;
        }

        for t in ready {
            last_active = Some(t.clone());
            busy.lock().unwrap().insert(t.channel_id.clone());
            spawn_turn(&shared, &busy, &data_dir, &mcp_cfg_path, t, WakeKind::Activity);
        }
        idle_deadline = idle_deadline_from(&shared, &last_active).await;
    }
}

async fn idle_deadline_from(shared: &Arc<Shared>, last: &Option<WakeTarget>) -> Instant {
    let m = match last {
        Some(t) => shared.behavior_for(&t.scope).await.idle_wake_minutes,
        None => 0,
    };
    let secs = if m > 0 { m * 60 } else { 365 * 24 * 3600 };
    Instant::now() + Duration::from_secs(secs)
}

fn spawn_turn(
    shared: &Arc<Shared>,
    busy: &Arc<Mutex<HashSet<String>>>,
    data_dir: &PathBuf,
    mcp_cfg_path: &PathBuf,
    target: WakeTarget,
    kind: WakeKind,
) {
    let (shared, busy) = (shared.clone(), busy.clone());
    let (data_dir, mcp_cfg_path) = (data_dir.clone(), mcp_cfg_path.clone());
    tokio::spawn(async move {
        channel_turn(&shared, &data_dir, &mcp_cfg_path, &target, kind).await;
        busy.lock().unwrap().remove(&target.channel_id);
        // New activity may have arrived while we were finishing up.
        shared.notify.notify_waiters();
    });
}

async fn channel_turn(
    shared: &Arc<Shared>,
    data_dir: &PathBuf,
    mcp_cfg_path: &PathBuf,
    target: &WakeTarget,
    kind: WakeKind,
) {
    let cfg = &shared.cfg.claude;
    if kind == WakeKind::Activity {
        sleep(Duration::from_secs(cfg.debounce_seconds)).await;
    }

    let gen_at_start = shared.scrub_gen.load(Ordering::SeqCst);
    let channel_id = &target.channel_id;
    let (st, first_time) = {
        let map = shared.sessions.lock().unwrap();
        (map.get(channel_id).cloned().unwrap_or_default(), !map.contains_key(channel_id))
    };
    let fresh = st.session_id.is_none() || st.wakes >= cfg.session_max_wakes;
    let persona_txt = persona::read_persona(shared, &target.scope).await;
    let sys = format!("{BASE_PROMPT}\n\n# Your persona (this server only)\n\n{persona_txt}");
    let place = format!("{} (channel_id {channel_id})", target.name);
    let body = if first_time && !target.is_dm {
        format!(
            "You were just summoned into {place} for the first time — an opted-in person \
             @mentioned you there. Call wait_for_messages to see the summons and respond \
             to it. Greet the room briefly; no need to lecture about privacy — your \
             standing consent post covers the opt-in mechanics. If someone's messages \
             reach you redacted or they seem confused about how you work, point them to \
             that post. Then behave as your persona sees fit."
        )
    } else if fresh {
        format!(
            "You just came online in {place} with a fresh session. Your persona is in \
             your system prompt; it is all you remember here, by design. Call \
             wait_for_messages to pick up whatever prompted this wake, and act as your \
             persona sees fit."
        )
    } else if kind == WakeKind::Idle {
        format!(
            "Idle wake in {place}: your idle timer fired — there may be no new messages. \
             Check in on things if you like (read_messages), tend to your persona notes, \
             then end your turn."
        )
    } else {
        format!(
            "You woke because of new activity in {place}. Call wait_for_messages to \
             receive it, then act (or don't) as your persona sees fit. End your turn \
             when things go quiet."
        )
    };

    let token = shared.new_token(TurnCtx {
        channel_id: channel_id.clone(),
        scope: target.scope.clone(),
        is_dm: target.is_dm,
    });
    let prompt = format!("{body}\n\nYour session token (pass as 'token' on every tool call): {token}");

    let mut cmd = Command::new(&cfg.binary);
    cmd.arg("-p")
        .arg(&prompt)
        .arg("--output-format")
        .arg("json")
        .arg("--model")
        .arg(&cfg.model)
        .arg("--mcp-config")
        .arg(mcp_cfg_path)
        .arg("--strict-mcp-config")
        .arg("--allowed-tools")
        .arg("mcp__discord")
        .arg("--append-system-prompt")
        .arg(&sys);
    if !cfg.disallowed_tools.is_empty() {
        cmd.arg("--disallowed-tools").args(&cfg.disallowed_tools);
    }
    if !fresh && let Some(id) = &st.session_id {
        cmd.arg("--resume").arg(id);
    }
    cmd.args(&cfg.extra_args);
    cmd.current_dir(data_dir)
        .env("MCP_TOOL_TIMEOUT", "600000")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    tracing::info!(channel = target.name, scope = target.scope, fresh, first_time, "waking claude");
    shared.typing_active.lock().unwrap().insert(channel_id.clone());
    let started = Instant::now();
    let ok = match cmd.spawn() {
        Err(e) => {
            tracing::error!("spawning {}: {e}", cfg.binary);
            false
        }
        // On timeout the child future is dropped → kill_on_drop reaps it.
        Ok(child) => match timeout(
            Duration::from_secs(cfg.turn_timeout_minutes * 60),
            child.wait_with_output(),
        )
        .await
        {
            Err(_) => {
                tracing::error!(channel = target.name, "turn exceeded {}m, killed", cfg.turn_timeout_minutes);
                false
            }
            Ok(Err(e)) => {
                tracing::error!("waiting on claude: {e}");
                false
            }
            Ok(Ok(out)) => {
                let stdout = String::from_utf8_lossy(&out.stdout);
                match serde_json::from_str::<Value>(stdout.trim()) {
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
                        let scrubbed = shared.scrub_gen.load(Ordering::SeqCst) != gen_at_start;
                        if scrubbed {
                            tracing::info!(channel = target.name, "scrub happened mid-turn; not recording session");
                        } else if let Some(id) = res["session_id"].as_str() {
                            let mut map = shared.sessions.lock().unwrap();
                            let e = map.entry(channel_id.clone()).or_default();
                            e.session_id = Some(id.to_string());
                            e.wakes = if fresh { 1 } else { e.wakes + 1 };
                        }
                        if !scrubbed {
                            shared.save_sessions();
                        }
                        !is_error
                    }
                    Err(e) => {
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        tracing::error!(
                            channel = target.name,
                            "unparseable claude output ({e}); stderr: {}",
                            stderr.chars().take(500).collect::<String>()
                        );
                        false
                    }
                }
            }
        },
    };
    shared.typing_active.lock().unwrap().remove(channel_id);
    shared.drop_token(&token);

    if ok {
        shared.sessions.lock().unwrap().entry(channel_id.clone()).or_default().failures = 0;
    } else {
        let failures = {
            let mut map = shared.sessions.lock().unwrap();
            let e = map.entry(channel_id.clone()).or_default();
            e.failures += 1;
            if e.failures >= 3 {
                // The session may be poisoned — start this channel fresh next time.
                tracing::warn!(channel = target.name, "3 consecutive failures, dropping session");
                e.session_id = None;
                e.wakes = 0;
                e.failures = 0;
            }
            e.failures
        };
        shared.save_sessions();
        // Back off while still holding this channel's busy slot.
        sleep(Duration::from_secs((2u64 << failures).min(60))).await;
    }
}
