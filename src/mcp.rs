//! Layer 2: a minimal MCP streamable-HTTP server exposing Discord tools
//! (and the layer-3 self-amendment tools) to Claude sessions.
//!
//! Every tool takes the turn's session token; the daemon resolves it to the
//! one channel + scope that turn may touch. Isolation across servers is
//! enforced here, not by prompting.

use crate::persona;
use crate::state::{Shared, TurnCtx};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use serenity::all::{ChannelId, ChannelType, MessageId, ReactionType};
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
    let out = match ctx_of(s, &args) {
        Err(e) => Err(e),
        Ok(ctx) => match name {
            "wait_for_messages" => wait_for_messages(s, &ctx, &args).await,
            "read_messages" => read_messages(s, &ctx, &args).await,
            "send_message" => send_message(s, &ctx, &args).await,
            "add_reaction" => add_reaction(s, &ctx, &args).await,
            "list_channels" => list_channels(s, &ctx).await,
            "get_persona" => Ok(json!({ "persona": persona::read_persona(s, &ctx.scope).await })),
            "set_persona" => set_persona(s, &ctx, &args).await,
            "get_behavior" => Ok(json!(s.behavior_for(&ctx.scope).await)),
            "set_behavior" => set_behavior(s, &ctx, &args).await,
            "get_consent" => get_consent(s, &ctx).await,
            "forget_user" => forget_user(s, &ctx, &args).await,
            "ignore_channel" => ignore_channel(s, &ctx).await,
            _ => return Err((-32602, format!("unknown tool: {name}"))),
        },
    };
    Ok(match out {
        Ok(v) => {
            let text = serde_json::to_string_pretty(&v).unwrap_or_else(|_| v.to_string());
            json!({ "content": [{ "type": "text", "text": text }] })
        }
        Err(e) => json!({ "content": [{ "type": "text", "text": e }], "isError": true }),
    })
}

fn ctx_of(s: &Arc<Shared>, args: &Value) -> Result<TurnCtx, String> {
    let t = args["token"]
        .as_str()
        .ok_or("missing 'token' — your session token is in your wake prompt")?;
    s.resolve_token(t).ok_or_else(|| "invalid or expired session token".to_string())
}

fn own_channel(ctx: &TurnCtx) -> Result<ChannelId, String> {
    Ok(ChannelId::new(parse_id(&ctx.channel_id)?))
}

// ---------- tools ----------

/// Unmarks "parked in wait" even if the request future is dropped mid-poll.
struct WaitGuard {
    s: Arc<Shared>,
    ch: String,
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        self.s.typing_waiting.lock().unwrap().remove(&self.ch);
    }
}

async fn wait_for_messages(s: &Arc<Shared>, ctx: &TurnCtx, args: &Value) -> Result<Value, String> {
    let secs = args["timeout_seconds"].as_u64().unwrap_or(240).clamp(5, 480);
    let deadline = Instant::now() + Duration::from_secs(secs);
    s.typing_waiting.lock().unwrap().insert(ctx.channel_id.clone());
    let _guard = WaitGuard { s: s.clone(), ch: ctx.channel_id.clone() };
    loop {
        // Register for wakeups *before* checking, so nothing slips between.
        let notified = s.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let evs = s.take_undelivered(&ctx.channel_id).await;
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

async fn read_messages(s: &Arc<Shared>, ctx: &TurnCtx, args: &Value) -> Result<Value, String> {
    let channel = own_channel(ctx)?;
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
    // History gets the same consent stripping as live messages: non-opted
    // people's messages are absent, not blanked.
    let opted = s
        .consent
        .read()
        .await
        .guilds
        .get(&ctx.scope)
        .map(|g| g.opted_users.clone())
        .unwrap_or_default();
    // Discord returns newest-first; flip to reading order.
    let list: Vec<Value> = msgs
        .iter()
        .rev()
        .filter(|m| {
            ctx.is_dm
                || m.author.bot
                || m.author.id.get() == bot_id
                || opted.contains_key(&m.author.id.to_string())
        })
        .map(|m| {
            json!({
                "message_id": m.id.to_string(),
                "author_name": m.author.name,
                "author_id": m.author.id.to_string(),
                "author_is_bot": m.author.bot,
                "is_me": m.author.id.get() == bot_id,
                "content": m.content,
                "timestamp": m.timestamp.to_string(),
                "reply_to_message_id": m.message_reference.as_ref()
                    .and_then(|r| r.message_id).map(|i| i.to_string()),
            })
        })
        .collect();
    Ok(json!({ "messages": list }))
}

async fn send_message(s: &Arc<Shared>, ctx: &TurnCtx, args: &Value) -> Result<Value, String> {
    let channel = own_channel(ctx)?;
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

async fn add_reaction(s: &Arc<Shared>, ctx: &TurnCtx, args: &Value) -> Result<Value, String> {
    let channel = own_channel(ctx)?;
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

async fn list_channels(s: &Arc<Shared>, ctx: &TurnCtx) -> Result<Value, String> {
    if ctx.is_dm {
        return Ok(json!({ "note": "this session is a DM — there is only this conversation" }));
    }
    let gid = serenity::all::GuildId::new(parse_id(&ctx.scope)?);
    let chans = s
        .http
        .get_channels(gid)
        .await
        .map_err(|e| format!("listing channels: {e}"))?;
    let active = s
        .consent
        .read()
        .await
        .guilds
        .get(&ctx.scope)
        .map(|g| g.active_channels.clone())
        .unwrap_or_default();
    let list: Vec<Value> = chans
        .iter()
        .filter(|c| matches!(c.kind, ChannelType::Text | ChannelType::News))
        .map(|c| {
            json!({
                "channel_id": c.id.to_string(),
                "name": c.name,
                "you_inhabit": active.contains(&c.id.to_string()),
            })
        })
        .collect();
    Ok(json!({
        "bot_user": { "id": s.bot_id.get().map(|i| i.to_string()), "name": s.bot_name.get() },
        "channels": list,
    }))
}

async fn set_persona(s: &Arc<Shared>, ctx: &TurnCtx, args: &Value) -> Result<Value, String> {
    let content = args["content"]
        .as_str()
        .filter(|c| !c.trim().is_empty())
        .ok_or("missing or empty 'content' — set_persona replaces the whole file")?;
    persona::write_persona(s, &ctx.scope, content).await?;
    // A new persona means a new mind: burn this scope's transcripts so the
    // next wake starts fresh with it.
    s.drop_scope_sessions(&ctx.scope).await;
    Ok(json!({
        "ok": true,
        "note": "persona saved. All sessions in this scope (including this one) reset: next \
                 wake starts a fresh session with the new persona, and transcript memory is \
                 gone. Make sure everything worth keeping is written in the persona itself."
    }))
}

async fn set_behavior(s: &Arc<Shared>, ctx: &TurnCtx, args: &Value) -> Result<Value, String> {
    let mut beh = s.behavior_for(&ctx.scope).await;
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
    s.behaviors.write().await.insert(ctx.scope.clone(), beh.clone());
    s.save_behaviors().await;
    Ok(json!({ "ok": true, "behavior": beh, "note": "in effect immediately, this scope only" }))
}

async fn get_consent(s: &Arc<Shared>, ctx: &TurnCtx) -> Result<Value, String> {
    if ctx.is_dm {
        return Ok(json!({ "note": "DMs are consent by definition; no registry here" }));
    }
    Ok(json!(s.consent.read().await.guilds.get(&ctx.scope).cloned().unwrap_or_default()))
}

/// Scrub a person from this server, at their request.
async fn forget_user(s: &Arc<Shared>, ctx: &TurnCtx, args: &Value) -> Result<Value, String> {
    if ctx.is_dm {
        // Forgetting a DM partner = wiping the DM itself.
        s.purge_channel(&ctx.channel_id).await;
        s.drop_scope_sessions(&ctx.scope).await;
        return Ok(json!({
            "ok": true,
            "note": "This DM's buffer and session are wiped. If your persona here mentions \
                     them, rewrite it via set_persona (or leave it — it is scoped to this \
                     DM only). End your turn now."
        }));
    }
    let user_id = args["user_id"].as_str().ok_or("missing 'user_id'")?;
    parse_id(user_id)?;
    let name = {
        let mut c = s.consent.write().await;
        c.guilds.entry(ctx.scope.clone()).or_default().opted_users.remove(user_id)
    };
    s.save_consent().await;
    s.purge_user(&ctx.scope, user_id).await;
    s.drop_scope_sessions(&ctx.scope).await;
    Ok(json!({
        "ok": true,
        "was_opted_in_as": name,
        "note": "User forgotten on this server: opted out, their buffered messages purged, \
                 and every session transcript here dropped (all channels start fresh next \
                 wake). IMPORTANT: now call get_persona and remove anything about this \
                 person via set_persona — that is the last place they could persist. Do \
                 this before ending your turn."
    }))
}

/// Return this session's channel to dormant.
async fn ignore_channel(s: &Arc<Shared>, ctx: &TurnCtx) -> Result<Value, String> {
    if ctx.is_dm {
        return Err("DMs can't be ignored — the person can simply stop writing, or ask you \
                    to forget them (forget_user)"
            .into());
    }
    {
        let mut c = s.consent.write().await;
        c.guilds.entry(ctx.scope.clone()).or_default().active_channels.remove(&ctx.channel_id);
    }
    s.save_consent().await;
    s.purge_channel(&ctx.channel_id).await;
    s.sessions.lock().unwrap().remove(&ctx.channel_id);
    s.scrub_gen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    s.save_sessions();
    Ok(json!({
        "ok": true,
        "note": "This channel is dormant again: you will see nothing from it unless an \
                 opted-in person @mentions you there. Its session is gone. Do not send \
                 anything further here — end your turn now."
    }))
}

// ---------- helpers ----------

fn parse_id(s: &str) -> Result<u64, String> {
    match s.trim().parse::<u64>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err(format!("bad Discord id: {s:?}")),
    }
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
    let tok = json!({ "type": "string", "description": "Your session token, from your wake prompt." });
    json!([
        {
            "name": "wait_for_messages",
            "description": "Block until new messages arrive in your channel (or the timeout passes), and return them. This is how you listen: call it to linger in an active conversation. An empty result means things are quiet — usually a good moment to end your turn.",
            "inputSchema": { "type": "object", "required": ["token"], "properties": {
                "token": tok.clone(),
                "timeout_seconds": { "type": "number", "description": "How long to wait before giving up (default 240, max 480)." }
            }}
        },
        {
            "name": "read_messages",
            "description": "Fetch recent message history for your channel (oldest first). Only opted-in people's messages appear; anyone else's are absent entirely.",
            "inputSchema": { "type": "object", "required": ["token"], "properties": {
                "token": tok.clone(),
                "limit": { "type": "number", "description": "1-50, default 20." },
                "before_message_id": { "type": "string", "description": "Page further back from this message id." }
            }}
        },
        {
            "name": "send_message",
            "description": "Send a message to your channel. Content over 2000 chars is split into multiple messages. Returns the sent message id(s).",
            "inputSchema": { "type": "object", "required": ["token", "content"], "properties": {
                "token": tok.clone(),
                "content": { "type": "string" },
                "reply_to_message_id": { "type": "string", "description": "Make this a reply to the given message." }
            }}
        },
        {
            "name": "add_reaction",
            "description": "React to a message in your channel. Emoji is a unicode emoji (e.g. \"🔥\") or a custom emoji as \"name:id\".",
            "inputSchema": { "type": "object", "required": ["token", "message_id", "emoji"], "properties": {
                "token": tok.clone(),
                "message_id": { "type": "string" },
                "emoji": { "type": "string" }
            }}
        },
        {
            "name": "list_channels",
            "description": "List this server's text channels (marking which you inhabit), plus your own bot identity.",
            "inputSchema": { "type": "object", "required": ["token"], "properties": { "token": tok.clone() } }
        },
        {
            "name": "get_persona",
            "description": "Read your persona for this server (your self-editable identity and long-term memory here).",
            "inputSchema": { "type": "object", "required": ["token"], "properties": { "token": tok.clone() } }
        },
        {
            "name": "set_persona",
            "description": "Replace your persona for this server, wholesale. It is your ONLY memory here. Saving it RESETS every session in this server (including this one) so the next wake starts fresh with the new persona — write down everything worth keeping.",
            "inputSchema": { "type": "object", "required": ["token", "content"], "properties": {
                "token": tok.clone(),
                "content": { "type": "string", "description": "The full new persona markdown." }
            }}
        },
        {
            "name": "get_behavior",
            "description": "Read this server's behavior settings: watched_channels, respond_to, idle_wake_minutes.",
            "inputSchema": { "type": "object", "required": ["token"], "properties": { "token": tok.clone() } }
        },
        {
            "name": "set_behavior",
            "description": "Adjust this server's behavior settings (effective immediately). watched_channels: channel ids to watch, empty = all inhabited. respond_to: \"mentions\" wakes you only for @mentions/DMs, \"all\" for any opted-in message. idle_wake_minutes: wake on a timer, 0 = off.",
            "inputSchema": { "type": "object", "required": ["token"], "properties": {
                "token": tok.clone(),
                "watched_channels": { "type": "array", "items": { "type": "string" } },
                "respond_to": { "type": "string", "enum": ["mentions", "all"] },
                "idle_wake_minutes": { "type": "number" }
            }}
        },
        {
            "name": "get_consent",
            "description": "Read this server's consent state: inhabited channels, opted-in people, and the consent post.",
            "inputSchema": { "type": "object", "required": ["token"], "properties": { "token": tok.clone() } }
        },
        {
            "name": "forget_user",
            "description": "Scrub a person from this server's memory, at their request: opts them out, purges their buffered messages, and drops every session transcript here (all channels start fresh next wake). Afterwards you MUST remove any notes about them from your persona via set_persona.",
            "inputSchema": { "type": "object", "required": ["token", "user_id"], "properties": {
                "token": tok.clone(),
                "user_id": { "type": "string", "description": "The Discord user id of the person asking to be forgotten." }
            }}
        },
        {
            "name": "ignore_channel",
            "description": "Leave YOUR channel, at its request: it returns to dormant — you see nothing from it until an opted-in person @mentions you there again, and its session is dropped. Say any goodbye BEFORE calling this; afterwards, end your turn.",
            "inputSchema": { "type": "object", "required": ["token"], "properties": { "token": tok.clone() } }
        }
    ])
}
