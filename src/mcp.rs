//! Layer 2: a minimal MCP streamable-HTTP server exposing Discord tools
//! (and the layer-3 self-amendment tools) to the Claude session.
//!
//! Only the message surface Claude Code actually uses is implemented:
//! initialize / ping / tools/list / tools/call, plain-JSON responses.

use crate::persona;
use crate::state::Shared;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use serenity::all::{Channel, ChannelId, ChannelType, MessageId, ReactionType, UserId};
use std::sync::atomic::Ordering;
use serenity::http::MessagePagination;
use std::sync::Arc;
use tokio::time::{Duration, Instant, sleep_until};

pub fn router(shared: Arc<Shared>) -> Router {
    Router::new()
        .route("/mcp", post(handle).get(async || StatusCode::METHOD_NOT_ALLOWED))
        .with_state(shared)
}

async fn handle(State(s): State<Arc<Shared>>, Json(req): Json<Value>) -> Response {
    let Some(id) = req.get("id").filter(|v| !v.is_null()).cloned() else {
        // Notification (e.g. notifications/initialized): accept and drop.
        return StatusCode::ACCEPTED.into_response();
    };
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let result: Result<Value, (i64, String)> = match method {
        "initialize" => Ok(json!({
            "protocolVersion": req
                .pointer("/params/protocolVersion")
                .cloned()
                .unwrap_or(json!("2025-06-18")),
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "bentham-discord", "version": env!("CARGO_PKG_VERSION") },
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_defs() })),
        "tools/call" => call_tool(&s, req.get("params").cloned().unwrap_or(json!({}))).await,
        _ => Err((-32601, format!("method not found: {method}"))),
    };
    let body = match result {
        Ok(r) => json!({ "jsonrpc": "2.0", "id": id, "result": r }),
        Err((code, message)) => {
            json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
        }
    };
    Json(body).into_response()
}

async fn call_tool(s: &Arc<Shared>, params: Value) -> Result<Value, (i64, String)> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or((-32602, "missing tool name".to_string()))?;
    let args = params.get("arguments").cloned().unwrap_or(json!({}));
    tracing::info!(tool = name, "tool call");
    let out = match name {
        "wait_for_messages" => wait_for_messages(s, &args).await,
        "read_messages" => read_messages(s, &args).await,
        "send_message" => send_message(s, &args).await,
        "add_reaction" => add_reaction(s, &args).await,
        "list_channels" => list_channels(s).await,
        "get_persona" => Ok(json!({ "persona": persona::read_persona(s).await })),
        "set_persona" => set_persona(s, &args).await,
        "get_behavior" => Ok(json!(s.behavior.read().await.clone())),
        "set_behavior" => set_behavior(s, &args).await,
        "get_consent" => Ok(json!(s.consent.read().await.clone())),
        "forget_user" => forget_user(s, &args).await,
        "ignore_channel" => ignore_channel(s, &args).await,
        _ => return Err((-32602, format!("unknown tool: {name}"))),
    };
    Ok(match out {
        Ok(v) => {
            let text = serde_json::to_string_pretty(&v).unwrap_or_else(|_| v.to_string());
            json!({ "content": [{ "type": "text", "text": text }] })
        }
        Err(e) => json!({ "content": [{ "type": "text", "text": e }], "isError": true }),
    })
}

// ---------- tools ----------

async fn wait_for_messages(s: &Arc<Shared>, args: &Value) -> Result<Value, String> {
    let channel = args["channel_id"]
        .as_str()
        .ok_or("missing 'channel_id' — pass the channel this session is bound to")?
        .to_string();
    let secs = args["timeout_seconds"].as_u64().unwrap_or(240).clamp(5, 480);
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        // Register for wakeups *before* checking, so nothing slips between.
        let notified = s.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let evs = s.take_undelivered(&channel).await;
        if !evs.is_empty() {
            return Ok(json!({ "messages": evs }));
        }
        tokio::select! {
            _ = notified => {}
            _ = sleep_until(deadline) => {
                return Ok(json!({
                    "messages": [],
                    "note": "timed out with no new messages — a good moment to end your turn"
                }));
            }
        }
    }
}

async fn read_messages(s: &Arc<Shared>, args: &Value) -> Result<Value, String> {
    let channel = channel_arg(args)?;
    let limit = args["limit"].as_u64().unwrap_or(20).clamp(1, 50) as u8;
    let before = match args["before_message_id"].as_str() {
        Some(m) => Some(MessagePagination::Before(MessageId::new(parse_id(m)?))),
        None => None,
    };
    let msgs = s
        .http
        .get_messages(channel, before, Some(limit))
        .await
        .map_err(|e| format!("fetching messages: {e}"))?;
    let bot_id = s.bot_id.get().copied().unwrap_or(0);
    // History gets the same consent redaction as live messages.
    let opted = s.consent.read().await.opted_users.clone();
    let is_dm = matches!(s.http.get_channel(channel).await, Ok(Channel::Private(_)));
    // Discord returns newest-first; flip to reading order.
    let list: Vec<Value> = msgs
        .iter()
        .rev()
        .map(|m| {
            let visible = is_dm
                || m.author.bot
                || m.author.id.get() == bot_id
                || opted.contains_key(&m.author.id.to_string())
                || (bot_id != 0 && m.mentions_user_id(UserId::new(bot_id)));
            json!({
                "message_id": m.id.to_string(),
                "author_name": m.author.name,
                "author_id": m.author.id.to_string(),
                "author_is_bot": m.author.bot,
                "is_me": m.author.id.get() == bot_id,
                "content": if visible { m.content.clone() } else { crate::discord::REDACTED.to_string() },
                "redacted": !visible,
                "timestamp": m.timestamp.to_string(),
                "reply_to_message_id": m.message_reference.as_ref()
                    .and_then(|r| r.message_id).map(|i| i.to_string()),
            })
        })
        .collect();
    Ok(json!({ "messages": list }))
}

async fn send_message(s: &Arc<Shared>, args: &Value) -> Result<Value, String> {
    let channel = channel_arg(args)?;
    let content = args["content"]
        .as_str()
        .filter(|c| !c.trim().is_empty())
        .ok_or("missing or empty 'content'")?;
    let reply_to = args["reply_to_message_id"].as_str();
    let mut ids = Vec::new();
    for (i, chunk) in split_chunks(content, 2000).iter().enumerate() {
        let mut payload = json!({ "content": chunk });
        if i == 0
            && let Some(r) = reply_to
        {
            payload["message_reference"] =
                json!({ "channel_id": channel.to_string(), "message_id": r });
        }
        let m = s
            .http
            .send_message(channel, vec![], &payload)
            .await
            .map_err(|e| format!("sending: {e}"))?;
        ids.push(m.id.to_string());
    }
    Ok(json!({ "sent_message_ids": ids }))
}

async fn add_reaction(s: &Arc<Shared>, args: &Value) -> Result<Value, String> {
    let channel = channel_arg(args)?;
    let msg = MessageId::new(parse_id(
        args["message_id"].as_str().ok_or("missing 'message_id'")?,
    )?);
    let emoji = args["emoji"].as_str().ok_or("missing 'emoji'")?;
    let reaction =
        ReactionType::try_from(emoji).map_err(|e| format!("bad emoji {emoji:?}: {e}"))?;
    s.http
        .create_reaction(channel, msg, &reaction)
        .await
        .map_err(|e| format!("reacting: {e}"))?;
    Ok(json!({ "ok": true }))
}

async fn list_channels(s: &Arc<Shared>) -> Result<Value, String> {
    let guilds = s
        .http
        .get_guilds(None, None)
        .await
        .map_err(|e| format!("listing guilds: {e}"))?;
    let mut out = Vec::new();
    for g in guilds {
        let chans = s.http.get_channels(g.id).await.unwrap_or_default();
        let chans: Vec<Value> = chans
            .iter()
            .filter(|c| matches!(c.kind, ChannelType::Text | ChannelType::News))
            .map(|c| json!({ "channel_id": c.id.to_string(), "name": c.name }))
            .collect();
        out.push(json!({ "guild": g.name, "channels": chans }));
    }
    Ok(json!({
        "bot_user": {
            "id": s.bot_id.get().map(|i| i.to_string()),
            "name": s.bot_name.get(),
        },
        "guilds": out,
    }))
}

async fn set_persona(s: &Arc<Shared>, args: &Value) -> Result<Value, String> {
    let content = args["content"]
        .as_str()
        .filter(|c| !c.trim().is_empty())
        .ok_or("missing or empty 'content' — set_persona replaces the whole file")?;
    persona::write_persona(s, content).await?;
    Ok(json!({ "ok": true, "note": "persona saved; it takes effect from your next wake" }))
}

async fn set_behavior(s: &Arc<Shared>, args: &Value) -> Result<Value, String> {
    let mut beh = s.behavior.read().await.clone();
    if let Some(w) = args.get("watched_channels").and_then(Value::as_array) {
        beh.watched_channels = w
            .iter()
            .map(|v| v.as_str().map(String::from).ok_or("watched_channels must be strings"))
            .collect::<Result<_, _>>()?;
    }
    if let Some(r) = args.get("respond_to").and_then(Value::as_str) {
        if r != "mentions" && r != "all" {
            return Err("respond_to must be \"mentions\" or \"all\"".into());
        }
        beh.respond_to = r.to_string();
    }
    if let Some(m) = args.get("idle_wake_minutes").and_then(Value::as_u64) {
        beh.idle_wake_minutes = m;
    }
    *s.behavior.write().await = beh.clone();
    persona::save_behavior(s).await.map_err(|e| e.to_string())?;
    Ok(json!({ "ok": true, "behavior": beh, "note": "in effect immediately" }))
}

/// Scrub a person on request: opt them out, purge their buffered messages,
/// and drop every channel's session transcript so nothing they said carries
/// forward. (Their persona-file traces are the caller's job — see the note.)
async fn forget_user(s: &Arc<Shared>, args: &Value) -> Result<Value, String> {
    let user_id = args["user_id"].as_str().ok_or("missing 'user_id'")?;
    parse_id(user_id)?;
    let name = s.consent.write().await.opted_users.remove(user_id);
    s.save_consent().await;
    s.purge_user(user_id).await;
    s.sessions.lock().unwrap().clear();
    s.scrub_gen.fetch_add(1, Ordering::SeqCst);
    s.save_sessions();
    Ok(json!({
        "ok": true,
        "was_opted_in_as": name,
        "note": "User forgotten: opted out, their buffered messages purged, and ALL channel \
                 session transcripts dropped (every channel, including this one, starts a \
                 fresh session next wake). IMPORTANT: now call get_persona and remove \
                 anything about this person via set_persona — that is the last place they \
                 could persist. Do this before ending your turn."
    }))
}

/// Return a channel to dormant: bentham sees nothing from it until someone
/// @mentions him there again.
async fn ignore_channel(s: &Arc<Shared>, args: &Value) -> Result<Value, String> {
    let channel_id = args["channel_id"].as_str().ok_or("missing 'channel_id'")?;
    parse_id(channel_id)?;
    let was_active = s.consent.write().await.active_channels.remove(channel_id);
    s.save_consent().await;
    s.purge_channel(channel_id).await;
    s.sessions.lock().unwrap().remove(channel_id);
    s.scrub_gen.fetch_add(1, Ordering::SeqCst);
    s.save_sessions();
    Ok(json!({
        "ok": true,
        "was_active": was_active,
        "note": "Channel is dormant again: you will see nothing from it unless someone \
                 @mentions you there. Its session is gone. Do not send anything further \
                 there — end your turn now."
    }))
}

// ---------- helpers ----------

fn parse_id(s: &str) -> Result<u64, String> {
    match s.trim().parse::<u64>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err(format!("bad Discord id: {s:?}")),
    }
}

fn channel_arg(args: &Value) -> Result<ChannelId, String> {
    Ok(ChannelId::new(parse_id(
        args["channel_id"].as_str().ok_or("missing 'channel_id'")?,
    )?))
}

/// Split into ≤max_chars chunks (Discord's limit is 2000), preferring newline breaks.
fn split_chunks(s: &str, max_chars: usize) -> Vec<String> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut start = 0;
    while start < chars.len() {
        let end = (start + max_chars).min(chars.len());
        let mut cut = end;
        if end < chars.len()
            && let Some(nl) = chars[start..end].iter().rposition(|&c| c == '\n')
            && nl > 0
        {
            cut = start + nl + 1;
        }
        out.push(chars[start..cut].iter().collect());
        start = cut;
    }
    out
}

fn tool_defs() -> Value {
    json!([
        {
            "name": "wait_for_messages",
            "description": "Block until new messages arrive in YOUR channel (or the timeout passes), and return them. This is how you listen: call it to linger in an active conversation. An empty result means things are quiet — usually a good moment to end your turn.",
            "inputSchema": { "type": "object", "required": ["channel_id"], "properties": {
                "channel_id": { "type": "string", "description": "The channel this session is bound to." },
                "timeout_seconds": { "type": "number", "description": "How long to wait before giving up (default 240, max 480)." }
            }}
        },
        {
            "name": "read_messages",
            "description": "Fetch recent message history for a channel (oldest first). Use for context; it does not affect what wait_for_messages returns.",
            "inputSchema": { "type": "object", "required": ["channel_id"], "properties": {
                "channel_id": { "type": "string" },
                "limit": { "type": "number", "description": "1-50, default 20." },
                "before_message_id": { "type": "string", "description": "Page further back from this message id." }
            }}
        },
        {
            "name": "send_message",
            "description": "Send a message to a channel or DM channel. Content over 2000 chars is split into multiple messages. Returns the sent message id(s).",
            "inputSchema": { "type": "object", "required": ["channel_id", "content"], "properties": {
                "channel_id": { "type": "string" },
                "content": { "type": "string" },
                "reply_to_message_id": { "type": "string", "description": "Make this a reply to the given message." }
            }}
        },
        {
            "name": "add_reaction",
            "description": "React to a message. Emoji is a unicode emoji (e.g. \"🔥\") or a custom emoji as \"name:id\".",
            "inputSchema": { "type": "object", "required": ["channel_id", "message_id", "emoji"], "properties": {
                "channel_id": { "type": "string" },
                "message_id": { "type": "string" },
                "emoji": { "type": "string" }
            }}
        },
        {
            "name": "list_channels",
            "description": "List the servers and text channels you can see, plus your own bot identity.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "get_persona",
            "description": "Read your persona file (your self-editable identity and long-term memory).",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "set_persona",
            "description": "Replace your persona file wholesale. It is injected into your system prompt at every wake, and it is your ONLY memory across session restarts — keep it current. Takes effect from your next wake.",
            "inputSchema": { "type": "object", "required": ["content"], "properties": {
                "content": { "type": "string", "description": "The full new persona markdown." }
            }}
        },
        {
            "name": "get_behavior",
            "description": "Read your behavior settings: watched_channels, respond_to, idle_wake_minutes.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "get_consent",
            "description": "Read the consent state: which channels you are active in, and who has opted in to you seeing their messages.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "forget_user",
            "description": "Scrub a person from your memory, at their request: opts them out, purges their buffered messages, and drops ALL channel session transcripts (every channel starts fresh next wake). Afterwards you MUST also remove any notes about them from your persona via set_persona.",
            "inputSchema": { "type": "object", "required": ["user_id"], "properties": {
                "user_id": { "type": "string", "description": "The Discord user id of the person asking to be forgotten." }
            }}
        },
        {
            "name": "ignore_channel",
            "description": "Leave a channel, at its request: it returns to dormant — you see nothing from it until someone @mentions you there again, and its session transcript is dropped. Say any goodbye BEFORE calling this; afterwards, end your turn.",
            "inputSchema": { "type": "object", "required": ["channel_id"], "properties": {
                "channel_id": { "type": "string" }
            }}
        },
        {
            "name": "set_behavior",
            "description": "Adjust your behavior settings (effective immediately). watched_channels: channel ids to watch, empty = all (DMs always watched). respond_to: \"mentions\" wakes you only for @mentions/DMs, \"all\" for any human message. idle_wake_minutes: wake on a timer even when quiet, 0 = off.",
            "inputSchema": { "type": "object", "properties": {
                "watched_channels": { "type": "array", "items": { "type": "string" } },
                "respond_to": { "type": "string", "enum": ["mentions", "all"] },
                "idle_wake_minutes": { "type": "number" }
            }}
        }
    ])
}
