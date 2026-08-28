//! Everything a session can do. Every tool requires the turn's session token,
//! which resolves to one channel in one scope — there is no argument that
//! reaches another channel, server, or persona.

use crate::state::{Shared, TurnCtx};
use crate::{consent, persona};
use serde_json::{Value, json};
use serenity::all::{ChannelId, MessageId, ReactionType};
use serenity::http::MessagePagination;
use std::sync::Arc;
use tokio::time::{Duration, Instant, sleep_until};

pub enum Error {
    UnknownTool,
    Failed(String),
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Error::Failed(e)
    }
}

impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Error::Failed(e.to_string())
    }
}

pub async fn dispatch(s: &Arc<Shared>, name: &str, args: &Value) -> Result<Value, Error> {
    let token = args["token"]
        .as_str()
        .ok_or("missing 'token' — your session token is in your wake prompt")?;
    let ctx = s.tokens.resolve(token).ok_or("invalid or expired session token")?;
    let out = match name {
        "wait_for_messages" => wait_for_messages(s, &ctx, args).await,
        "read_messages" => read_messages(s, &ctx, args).await,
        "send_message" => send_message(s, &ctx, args).await,
        "add_reaction" => add_reaction(s, &ctx, args).await,
        "get_persona" => Ok(json!({ "persona": persona::read(s, &ctx.scope).await })),
        "set_persona" => set_persona(s, &ctx, args).await,
        "get_behavior" => Ok(json!(persona::behavior_for(s, &ctx.scope).await)),
        "set_behavior" => set_behavior(s, &ctx, args).await,
        "get_consent" => get_consent(s, &ctx).await,
        "ignore_channel" => ignore_channel(s, &ctx).await,
        _ => return Err(Error::UnknownTool),
    };
    out.map_err(Error::Failed)
}

/// Unmarks "parked in wait" even if the request future is dropped mid-poll.
struct WaitGuard {
    s: Arc<Shared>,
    ch: String,
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        self.s.typing.wait_ended(&self.ch);
    }
}

async fn wait_for_messages(s: &Arc<Shared>, ctx: &TurnCtx, args: &Value) -> Result<Value, String> {
    let secs = args["timeout_seconds"].as_u64().unwrap_or(240).clamp(5, 480);
    let deadline = Instant::now() + Duration::from_secs(secs);
    s.typing.wait_started(&ctx.channel_id);
    let _guard = WaitGuard { s: s.clone(), ch: ctx.channel_id.clone() };
    loop {
        // Register for wakeups *before* checking, so nothing slips between.
        let notified = s.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let evs = s.buffer.take_undelivered(&ctx.channel_id).await;
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
    // History gets the same consent gate as live messages: non-opted people's
    // messages are absent, not blanked.
    let opted = consent::guild(s, &ctx.scope).await.opted_users;
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
    if ctx.maintenance {
        return Err("this is a maintenance turn — messaging is disabled".into());
    }
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
    if ctx.maintenance {
        return Err("this is a maintenance turn — messaging is disabled".into());
    }
    let channel = own_channel(ctx)?;
    let msg = MessageId::new(parse_id(args["message_id"].as_str().ok_or("missing 'message_id'")?)?);
    let emoji = args["emoji"].as_str().ok_or("missing 'emoji'")?;
    let reaction = ReactionType::try_from(emoji).map_err(|e| format!("bad emoji {emoji:?}: {e}"))?;
    s.http
        .create_reaction(channel, msg, &reaction)
        .await
        .map_err(|e| format!("reacting: {e}"))?;
    Ok(json!({ "ok": true }))
}

async fn set_persona(s: &Arc<Shared>, ctx: &TurnCtx, args: &Value) -> Result<Value, String> {
    let content = args["content"]
        .as_str()
        .filter(|c| !c.trim().is_empty())
        .ok_or("missing or empty 'content' — set_persona replaces the whole file")?;
    persona::write(s, &ctx.scope, content).await?;
    // A new persona means a new mind: transcripts made under the old one die.
    s.drop_scope_sessions(&ctx.scope).await;
    Ok(json!({
        "ok": true,
        "note": "persona saved. All sessions in this scope (including this one) reset: next \
                 wake starts a fresh session with the new persona, and transcript memory is \
                 gone. Make sure everything worth keeping is written in the persona itself."
    }))
}

async fn set_behavior(s: &Arc<Shared>, ctx: &TurnCtx, args: &Value) -> Result<Value, String> {
    let mut beh = persona::behavior_for(s, &ctx.scope).await;
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
    persona::save_behaviors(s).await;
    Ok(json!({ "ok": true, "behavior": beh, "note": "in effect immediately, this scope only" }))
}

async fn get_consent(s: &Arc<Shared>, ctx: &TurnCtx) -> Result<Value, String> {
    if ctx.is_dm {
        return Ok(json!({ "note": "DMs are consent by definition; no registry here" }));
    }
    Ok(json!(consent::guild(s, &ctx.scope).await))
}

/// Return this session's channel to dormant.
async fn ignore_channel(s: &Arc<Shared>, ctx: &TurnCtx) -> Result<Value, String> {
    if ctx.is_dm {
        return Err("DMs can't be ignored — the person can simply stop writing".into());
    }
    {
        let mut c = s.consent.write().await;
        c.guilds.entry(ctx.scope.clone()).or_default().active_channels.remove(&ctx.channel_id);
    }
    consent::save(s).await;
    s.buffer.purge_channel(&ctx.channel_id).await;
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

fn parse_id(s: &str) -> Result<u64, String> {
    match s.trim().parse::<u64>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err(format!("bad Discord id: {s:?}")),
    }
}

fn own_channel(ctx: &TurnCtx) -> Result<ChannelId, String> {
    Ok(ChannelId::new(parse_id(&ctx.channel_id)?))
}

/// ≤max_chars chunks (Discord caps messages at 2000), preferring newline breaks.
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

pub fn definitions() -> Value {
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
            "name": "ignore_channel",
            "description": "Leave YOUR channel, at its request: it returns to dormant — you see nothing from it until an opted-in person @mentions you there again, and its session is dropped. Say any goodbye BEFORE calling this; afterwards, end your turn.",
            "inputSchema": { "type": "object", "required": ["token"], "properties": { "token": tok.clone() } }
        }
    ])
}
