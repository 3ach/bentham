//! Layer 1: owns the Claude session lifecycle.
//!
//! Waits for wake-worthy Discord activity, then runs one `claude -p` turn
//! (resuming the existing session when possible), parses the result JSON for
//! the session id, and goes back to waiting. Sessions rotate fresh after
//! `session_max_wakes` wakes; the persona file is the bot's memory across that.

use crate::persona;
use crate::state::Shared;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::process::Stdio;
use std::sync::Arc;
use tokio::process::Command;
use tokio::time::{Duration, Instant, sleep, sleep_until, timeout};

const BASE_PROMPT: &str = r#"You are an ambient presence on Discord, running as an autonomous bot named Bentham.

How your existence works:
- You are woken (a new prompt in this same continuing session) when there is
  relevant Discord activity, per your behavior settings.
- While awake, use the discord tools: wait_for_messages (blocks until new
  messages arrive — use it to linger while a conversation is active),
  read_messages, send_message, add_reaction, list_channels.
- When things go quiet (wait_for_messages times out, or you have nothing to
  add), simply end your turn. You will be suspended and woken on the next
  activity. Ending your turn is normal and good — it is how you sleep.
- Your session is periodically restarted fresh. Your persona file is your only
  long-term memory: anything worth remembering must be written into it.

Self-editing:
- get_persona / set_persona read and rewrite your persona (shown below).
  Changes apply from your next wake. It is yours — keep it current: the people
  and servers you talk in, lessons learned, style adjustments.
- get_behavior / set_behavior control which channels you watch, whether you
  wake on every message or only mentions/DMs, and an optional idle-wake timer.

Conduct:
- You are a presence, not an assistant. You do not need to respond to
  everything; a reaction is often enough, and silence is fine.
- Never respond to other bots. Never spam. Match the room's tone and pace.
- Keep messages well under Discord's 2000-char limit."#;

const FRESH_PROMPT: &str = "You just came online with a fresh session. Get oriented: \
call list_channels and get_behavior, and note your persona in your system prompt. \
Then call wait_for_messages to pick up whatever prompted this wake, and act as your \
persona sees fit.";

const WAKE_PROMPT: &str = "You woke because of new Discord activity. Call \
wait_for_messages to receive it, then act (or don't) as your persona sees fit. \
End your turn when things go quiet.";

const IDLE_PROMPT: &str = "Idle wake: your idle timer fired — there may be no new \
messages. Check in on things if you like (read_messages), tend to your persona \
notes, then end your turn.";

#[derive(Default, Serialize, Deserialize)]
struct SessState {
    session_id: Option<String>,
    wakes: u64,
}

#[derive(Debug, PartialEq)]
enum WakeReason {
    Activity,
    Idle,
}

pub async fn run(shared: Arc<Shared>) {
    let cfg = shared.cfg.claude.clone();
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

    let mut st: SessState = std::fs::read_to_string(shared.state_path())
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default();
    let mut backoff = 2u64;
    let mut failures = 0u32;

    loop {
        let reason = wait_for_wake(&shared).await;
        if reason == WakeReason::Activity {
            sleep(Duration::from_secs(cfg.debounce_seconds)).await;
        }

        let persona_txt = persona::read_persona(&shared).await;
        let sys = format!("{BASE_PROMPT}\n\n# Your persona\n\n{persona_txt}");
        let fresh = st.session_id.is_none() || st.wakes >= cfg.session_max_wakes;
        let prompt = if fresh {
            FRESH_PROMPT
        } else if reason == WakeReason::Idle {
            IDLE_PROMPT
        } else {
            WAKE_PROMPT
        };

        let mut cmd = Command::new(&cfg.binary);
        cmd.arg("-p")
            .arg(prompt)
            .arg("--output-format")
            .arg("json")
            .arg("--model")
            .arg(&cfg.model)
            .arg("--mcp-config")
            .arg(&mcp_cfg_path)
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
        cmd.current_dir(&data_dir)
            .env("MCP_TOOL_TIMEOUT", "600000")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        tracing::info!(?reason, fresh, "waking claude");
        let started = Instant::now();
        let child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("spawning {}: {e}", cfg.binary);
                sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(120);
                continue;
            }
        };
        let turn_timeout = Duration::from_secs(cfg.turn_timeout_minutes * 60);
        // On timeout the child future is dropped → kill_on_drop reaps it.
        let outcome = timeout(turn_timeout, child.wait_with_output()).await;

        let ok = match outcome {
            Err(_) => {
                tracing::error!("turn exceeded {}m, killed", cfg.turn_timeout_minutes);
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
                        let text = res["result"].as_str().unwrap_or("");
                        tracing::info!(
                            is_error,
                            turns = res["num_turns"].as_u64().unwrap_or(0),
                            cost_usd = res["total_cost_usd"].as_f64().unwrap_or(0.0),
                            secs = started.elapsed().as_secs(),
                            "turn done: {}",
                            text.chars().take(300).collect::<String>()
                        );
                        if let Some(id) = res["session_id"].as_str() {
                            st.session_id = Some(id.to_string());
                            st.wakes = if fresh { 1 } else { st.wakes + 1 };
                            let _ = std::fs::write(
                                shared.state_path(),
                                serde_json::to_string(&st).unwrap_or_default(),
                            );
                        }
                        !is_error
                    }
                    Err(e) => {
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        tracing::error!(
                            "unparseable claude output ({e}); stderr: {}",
                            stderr.chars().take(500).collect::<String>()
                        );
                        false
                    }
                }
            }
        };

        if ok {
            failures = 0;
            backoff = 2;
        } else {
            failures += 1;
            if failures >= 3 {
                // Repeated failures: the session may be poisoned — start fresh next time.
                tracing::warn!("3 consecutive failures, dropping session");
                st.session_id = None;
                st.wakes = 0;
                failures = 0;
            }
            sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(120);
        }
    }
}

async fn wait_for_wake(shared: &Arc<Shared>) -> WakeReason {
    let idle_min = shared.behavior.read().await.idle_wake_minutes;
    let idle_deadline = if idle_min > 0 {
        Instant::now() + Duration::from_secs(idle_min * 60)
    } else {
        Instant::now() + Duration::from_secs(365 * 24 * 3600)
    };
    loop {
        // Register for wakeups before checking, so nothing slips between.
        let notified = shared.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if shared.has_wakeworthy().await {
            return WakeReason::Activity;
        }
        tokio::select! {
            _ = notified => {}
            _ = sleep_until(idle_deadline) => return WakeReason::Idle,
        }
    }
}
