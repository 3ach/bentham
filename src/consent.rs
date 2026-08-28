//! Who may be seen. The reaction on each server's consent post is the source
//! of truth: react = opt in, un-react = opt out. Opting out triggers the full
//! scrub pipeline. Nothing in this file is shared across servers.

use crate::state::{ScrubJob, Shared};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serenity::all::{ChannelId, ChannelType, Guild, MessageId};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Consent {
    #[serde(default)]
    pub guilds: HashMap<String, GuildConsent>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct GuildConsent {
    /// Channels the bot inhabits (summoned by an opted-in @mention).
    /// Messages anywhere else in the server are dropped at ingest.
    #[serde(default)]
    pub active_channels: HashSet<String>,
    /// user id -> display name at opt-in time.
    #[serde(default)]
    pub opted_users: HashMap<String, String>,
    #[serde(default)]
    pub consent_post: Option<ConsentPost>,
    /// Opted in by the operator without a reaction on record: the reconciler
    /// won't opt them out, but a live un-react still does.
    #[serde(default)]
    pub grandfathered: HashSet<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ConsentPost {
    pub channel_id: String,
    pub message_id: String,
}

pub async fn load(shared: &Shared) -> anyhow::Result<()> {
    let path = shared.consent_path();
    if path.exists() {
        match serde_json::from_str(&tokio::fs::read_to_string(&path).await?) {
            Ok(c) => *shared.consent.write().await = c,
            Err(e) => tracing::warn!("ignoring corrupt {}: {e}", path.display()),
        }
    }
    Ok(())
}

pub async fn save(shared: &Shared) {
    let c = shared.consent.read().await.clone();
    let text = serde_json::to_string_pretty(&c).unwrap_or_default();
    let _ = tokio::fs::write(shared.consent_path(), text).await;
}

pub async fn guild(shared: &Shared, gid: &str) -> GuildConsent {
    shared.consent.read().await.guilds.get(gid).cloned().unwrap_or_default()
}

/// Idempotent; returns true if newly opted in.
pub async fn opt_in(shared: &Shared, gid: &str, user_id: &str, name: &str) -> bool {
    let newly = {
        let mut c = shared.consent.write().await;
        c.guilds
            .entry(gid.to_string())
            .or_default()
            .opted_users
            .insert(user_id.to_string(), name.to_string())
            .is_none()
    };
    if newly {
        save(shared).await;
    }
    newly
}

/// The full opt-out pipeline: consent entry removed, this server's session
/// transcripts dropped, their buffered messages purged, and a persona scrub
/// queued (supervisor.rs runs it). Returns their name if they were opted in.
pub async fn opt_out(shared: &Shared, gid: &str, user_id: &str) -> Option<String> {
    let removed = {
        let mut c = shared.consent.write().await;
        c.guilds.entry(gid.to_string()).or_default().opted_users.remove(user_id)
    };
    let name = removed?;
    save(shared).await;
    shared.drop_scope_sessions(gid).await;
    shared.buffer.purge_user(gid, user_id).await;
    {
        let mut q = shared.pending_scrubs.lock().unwrap();
        let job = ScrubJob {
            scope: gid.to_string(),
            user_id: user_id.to_string(),
            user_name: name.clone(),
        };
        if !q.contains(&job) {
            q.push(job);
        }
    }
    shared.notify.notify_waiters();
    Some(name)
}

// ---- the consent post ----

/// First line doubles as the marker for re-finding the post after data loss.
const MARKER: &str = "\u{1F44B} I'm bentham";

const POST: &str = "\u{1F44B} I'm bentham, an AI presence on this server. How privacy works with me:\n\
\u{2022} I can only read messages from people who **opt in** \u{2014} react to **this message** with any emoji to opt in.\n\
\u{2022} Remove your reaction any time to opt out \u{2014} I then automatically forget you: my sessions reset and my notes about you are scrubbed.\n\
\u{2022} I only inhabit channels where an opted-in person @mentions me; everywhere else I see nothing \u{2014} including @mentions from people who haven't opted in.\n\
\u{2022} Ask me to leave a channel and I'll go.\n\
I simply never receive messages from anyone who hasn't opted in \u{2014} they aren't hidden from me, they never reach me at all.\n\
What I learn on this server stays on this server.";

fn post_text() -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{POST}\n\n-# last restart: <t:{ts}:f>")
}

pub async fn post_of(shared: &Shared, gid: &str) -> Option<ConsentPost> {
    shared.consent.read().await.guilds.get(gid).and_then(|g| g.consent_post.clone())
}

pub async fn is_post(shared: &Shared, gid: &str, message_id: u64) -> bool {
    post_of(shared, gid).await.is_some_and(|p| p.message_id == message_id.to_string())
}

async fn record_post(shared: &Shared, gid: &str, channel: ChannelId, message_id: String) {
    shared
        .consent
        .write()
        .await
        .guilds
        .entry(gid.to_string())
        .or_default()
        .consent_post = Some(ConsentPost { channel_id: channel.to_string(), message_id });
    save(shared).await;
}

/// On guild join / startup: refresh the known post's footer in place, or
/// re-find / create it. Posting is not watching — the channel stays dormant.
pub async fn refresh_post(shared: &Shared, guild: &Guild) {
    let gid = guild.id.to_string();
    if let Some(post) = post_of(shared, &gid).await {
        let (Ok(ch), Ok(mid)) = (post.channel_id.parse::<u64>(), post.message_id.parse::<u64>())
        else {
            return;
        };
        let edit = shared
            .http
            .edit_message(
                ChannelId::new(ch),
                MessageId::new(mid),
                &json!({ "content": post_text() }),
                vec![],
            )
            .await;
        if edit.is_ok() {
            tracing::info!(guild = gid, "refreshed consent post footer");
            return;
        }
        tracing::warn!(guild = gid, "recorded consent post gone; re-finding");
    }
    let text_chans: Vec<_> = guild
        .channels
        .values()
        .filter(|c| matches!(c.kind, ChannelType::Text | ChannelType::News))
        .collect();
    let target = text_chans
        .iter()
        .find(|c| c.name == "general")
        .map(|c| c.id)
        .or(guild.system_channel_id)
        .or_else(|| text_chans.first().map(|c| c.id));
    match target {
        Some(t) => ensure_post(shared, &gid, t).await,
        None => tracing::warn!(guild = gid, "no text channel for consent notice"),
    }
}

/// Adopt an existing post of ours (found by its marker line) before ever
/// posting fresh — restarts and data loss must not spam the channel.
pub async fn ensure_post(shared: &Shared, gid: &str, fallback: ChannelId) {
    if post_of(shared, gid).await.is_some() {
        return;
    }
    let _one_at_a_time = shared.consent_post_lock.lock().await;
    if post_of(shared, gid).await.is_some() {
        return;
    }
    let target = match gid.parse::<u64>() {
        Ok(g) => shared
            .http
            .get_channels(serenity::all::GuildId::new(g))
            .await
            .ok()
            .and_then(|chans| chans.into_iter().find(|c| c.name == "general").map(|c| c.id))
            .unwrap_or(fallback),
        Err(_) => fallback,
    };
    let bot_id = shared.bot_id.get().copied().unwrap_or(0);
    if let Ok(msgs) = shared.http.get_messages(target, None, Some(50)).await
        && let Some(m) = msgs
            .iter()
            .find(|m| m.author.id.get() == bot_id && m.content.starts_with(MARKER))
    {
        let _ = shared
            .http
            .edit_message(target, m.id, &json!({ "content": post_text() }), vec![])
            .await;
        record_post(shared, gid, target, m.id.to_string()).await;
        tracing::info!(guild = gid, "adopted existing consent post");
        return;
    }
    match shared.http.send_message(target, vec![], &json!({ "content": post_text() })).await {
        Ok(m) => {
            record_post(shared, gid, target, m.id.to_string()).await;
            tracing::info!(guild = gid, channel = target.to_string(), "posted consent notice");
        }
        Err(e) => tracing::warn!(guild = gid, "couldn't post consent notice: {e}"),
    }
}

/// Poll each consent post's reactions and reconcile both directions — catches
/// anything that happened while the daemon was down. Fails safe: an unreadable
/// post changes nothing.
pub async fn reconcile(shared: std::sync::Arc<Shared>) {
    loop {
        tokio::time::sleep(Duration::from_secs(180)).await;
        let posts: Vec<(String, ConsentPost)> = shared
            .consent
            .read()
            .await
            .guilds
            .iter()
            .filter_map(|(g, gc)| gc.consent_post.clone().map(|p| (g.clone(), p)))
            .collect();
        for (gid, post) in posts {
            let (Ok(ch), Ok(mid)) = (post.channel_id.parse::<u64>(), post.message_id.parse::<u64>())
            else {
                continue;
            };
            let (ch, mid) = (ChannelId::new(ch), MessageId::new(mid));
            let msg = match shared.http.get_message(ch, mid).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(guild = gid, "reconcile: can't fetch consent post: {e}");
                    continue;
                }
            };
            let bot_id = shared.bot_id.get().copied().unwrap_or(0);
            let mut reacted: HashMap<String, String> = HashMap::new();
            for r in &msg.reactions {
                match shared.http.get_reaction_users(ch, mid, &r.reaction_type, 100, None).await {
                    Ok(users) => {
                        for u in users {
                            if !u.bot && u.id.get() != bot_id {
                                reacted.insert(u.id.to_string(), u.name.clone());
                            }
                        }
                    }
                    Err(e) => tracing::warn!(guild = gid, "reconcile: reaction fetch: {e}"),
                }
            }
            for (uid, name) in &reacted {
                if opt_in(&shared, &gid, uid, name).await {
                    tracing::info!(user = name, guild = gid, "opted in (reconciled)");
                }
            }
            let g = guild(&shared, &gid).await;
            for uid in g.opted_users.keys() {
                if !reacted.contains_key(uid)
                    && !g.grandfathered.contains(uid)
                    && let Some(name) = opt_out(&shared, &gid, uid).await
                {
                    tracing::info!(user = name, guild = gid, "opted out (reconciled)");
                }
            }
        }
    }
}
