use crate::state::{ConsentPost, MsgEvent, ScrubJob, Shared};
use serde_json::json;
use serenity::all::{
    ChannelId, ChannelType, Context, EventHandler, GatewayIntents, Guild, Message, Reaction,
    Ready, UserId,
};
use serenity::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// First line doubles as the marker for re-finding the post after data loss.
const CONSENT_MARKER: &str = "\u{1F44B} I'm bentham";

const CONSENT_POST: &str = "\u{1F44B} I'm bentham, an AI presence on this server. How privacy works with me:\n\
\u{2022} I can only read messages from people who **opt in** \u{2014} react to **this message** with any emoji to opt in.\n\
\u{2022} Remove your reaction any time to opt back out.\n\
\u{2022} I only inhabit channels where an opted-in person @mentions me; everywhere else I see nothing \u{2014} including @mentions from people who haven't opted in.\n\
\u{2022} Ask me to forget you and I'll scrub you from my memory here. Ask me to leave a channel and I'll go.\n\
I simply never receive messages from anyone who hasn't opted in \u{2014} they aren't hidden from me, they never reach me at all.\n\
What I learn on this server stays on this server.";

fn consent_content() -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{CONSENT_POST}\n\n-# last restart: <t:{ts}:f>")
}

struct Handler {
    shared: Arc<Shared>,
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, _ctx: Context, data: Ready) {
        let _ = self.shared.bot_id.set(data.user.id.get());
        let _ = self.shared.bot_name.set(data.user.name.clone());
        tracing::info!("discord ready as {} ({})", data.user.name, data.user.id);
    }

    async fn message(&self, ctx: Context, msg: Message) {
        let bot_id = self.shared.bot_id.get().copied().unwrap_or(0);
        if msg.author.id.get() == bot_id {
            return;
        }
        let is_dm = msg.guild_id.is_none();
        let mentions_me = bot_id != 0 && msg.mentions_user_id(UserId::new(bot_id));
        let channel_id = msg.channel_id.to_string();
        let author_id = msg.author.id.to_string();

        let scope = if is_dm {
            // DMing bentham is consent; each DM is its own isolation scope.
            format!("dm-{channel_id}")
        } else {
            let gid = msg.guild_id.map(|g| g.to_string()).unwrap_or_default();
            let (active, opted) = {
                let c = self.shared.consent.read().await;
                match c.guilds.get(&gid) {
                    Some(g) => (
                        g.active_channels.contains(&channel_id),
                        g.opted_users.contains_key(&author_id),
                    ),
                    None => (false, false),
                }
            };
            // Consent gate 1: dormant channels are invisible until an
            // opted-in person @mentions bentham there — mentions carry no
            // privileges for anyone else.
            if !active {
                if !(mentions_me && opted) {
                    return; // never buffered, never seen
                }
                self.shared
                    .consent
                    .write()
                    .await
                    .guilds
                    .entry(gid.clone())
                    .or_default()
                    .active_channels
                    .insert(channel_id.clone());
                self.shared.save_consent().await;
                tracing::info!(channel = channel_id, guild = gid, "summoned into new channel");
            }
            // Backstop; the consent post is normally created on guild join.
            self.ensure_consent_post(&gid, msg.channel_id).await;
            // Consent gate 2: non-opted humans are stripped at ingest — their
            // messages (and their @mentions of bentham) simply never reach
            // him. Other bots have no privacy interest.
            if !msg.author.bot && !opted {
                return;
            }
            gid
        };

        let content = {
            let mut c = msg.content.clone();
            for a in &msg.attachments {
                c.push_str(&format!("\n[attachment: {}]", a.url));
            }
            c
        };

        let channel_name = msg.channel_id.name(&ctx).await.ok();
        let guild_name = msg.guild_id.and_then(|g| g.name(&ctx.cache));

        let ev = MsgEvent {
            seq: 0, // assigned by push_event
            message_id: msg.id.to_string(),
            channel_id,
            channel_name,
            guild_name,
            is_dm,
            author_id,
            author_name: msg.author.name.clone(),
            author_is_bot: msg.author.bot,
            content,
            timestamp: msg.timestamp.to_string(),
            mentions_me,
            reply_to_message_id: msg
                .referenced_message
                .as_ref()
                .map(|m| m.id.to_string())
                .or_else(|| {
                    msg.message_reference
                        .as_ref()
                        .and_then(|r| r.message_id)
                        .map(|id| id.to_string())
                }),
            scope,
        };
        self.shared.push_event(ev).await;
    }

    /// Fires on join and once per guild at startup: make sure the standing
    /// consent post exists (opting in via it is the only door into bentham),
    /// and refresh its "last restart" footer. Never reposts if the existing
    /// post can be edited or re-found by its marker line.
    async fn guild_create(&self, _ctx: Context, guild: Guild, _is_new: Option<bool>) {
        let gid = guild.id.to_string();
        // Known post: just refresh the footer in place.
        if let Some(post) = self.consent_post_of(&gid).await {
            let (Ok(ch), Ok(mid)) = (post.channel_id.parse::<u64>(), post.message_id.parse::<u64>())
            else {
                return;
            };
            if self
                .shared
                .http
                .edit_message(
                    ChannelId::new(ch),
                    serenity::all::MessageId::new(mid),
                    &json!({ "content": consent_content() }),
                    vec![],
                )
                .await
                .is_ok()
            {
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
            Some(t) => self.ensure_consent_post(&gid, t).await,
            None => tracing::warn!(guild = gid, "no text channel for consent notice"),
        }
    }

    /// Reacting to the server's consent post = opting in (to that server only).
    async fn reaction_add(&self, _ctx: Context, r: Reaction) {
        let bot_id = self.shared.bot_id.get().copied().unwrap_or(0);
        let (Some(gid), Some(user_id)) = (r.guild_id, r.user_id) else { return };
        if bot_id == 0 || user_id.get() == bot_id {
            return;
        }
        let gid = gid.to_string();
        if !self.is_consent_post(&gid, r.message_id.get()).await {
            return;
        }
        let name = match &r.member {
            Some(m) => m.user.name.clone(),
            None => match self.shared.http.get_user(user_id).await {
                Ok(u) => u.name,
                Err(_) => user_id.to_string(),
            },
        };
        self.shared
            .consent
            .write()
            .await
            .guilds
            .entry(gid.clone())
            .or_default()
            .opted_users
            .insert(user_id.to_string(), name.clone());
        self.shared.save_consent().await;
        tracing::info!(user = name, guild = gid, "opted in");
    }

    /// Removing a reaction from the consent post = opting back out.
    async fn reaction_remove(&self, _ctx: Context, r: Reaction) {
        let bot_id = self.shared.bot_id.get().copied().unwrap_or(0);
        let (Some(gid), Some(user_id)) = (r.guild_id, r.user_id) else { return };
        if bot_id == 0 || user_id.get() == bot_id {
            return;
        }
        let gid = gid.to_string();
        if !self.is_consent_post(&gid, r.message_id.get()).await {
            return;
        }
        // NB: bind first — an `if let` scrutinee keeps its temporaries (the
        // write guard) alive through the success block, which would deadlock
        // against save_consent's read lock.
        let removed = {
            let mut c = self.shared.consent.write().await;
            c.guilds.entry(gid.clone()).or_default().opted_users.remove(&user_id.to_string())
        };
        if let Some(name) = removed {
            self.shared.save_consent().await;
            // Opting out burns this server's session transcripts and buffered
            // messages, and queues a maintenance turn to scrub persona notes.
            self.shared.drop_scope_sessions(&gid).await;
            self.shared.purge_user(&gid, &user_id.to_string()).await;
            self.shared.pending_scrubs.lock().unwrap().push(ScrubJob {
                scope: gid.clone(),
                user_id: user_id.to_string(),
                user_name: name.clone(),
            });
            self.shared.notify.notify_waiters();
            tracing::info!(user = name, guild = gid, "opted out; sessions dropped, scrub queued");
        }
    }
}

impl Handler {
    async fn consent_post_of(&self, guild_id: &str) -> Option<ConsentPost> {
        self.shared
            .consent
            .read()
            .await
            .guilds
            .get(guild_id)
            .and_then(|g| g.consent_post.clone())
    }

    async fn is_consent_post(&self, guild_id: &str, message_id: u64) -> bool {
        self.shared
            .consent
            .read()
            .await
            .guilds
            .get(guild_id)
            .and_then(|g| g.consent_post.as_ref())
            .is_some_and(|p| p.message_id == message_id.to_string())
    }

    /// Make sure the server's one standing consent notice exists: adopt an
    /// existing post found by its marker line if the record was lost, and
    /// only post fresh as a last resort. Posting is not watching: the target
    /// channel stays dormant.
    async fn ensure_consent_post(&self, guild_id: &str, fallback: ChannelId) {
        if self.consent_post_of(guild_id).await.is_some() {
            return;
        }
        let _guard = self.shared.consent_post_lock.lock().await;
        if self.consent_post_of(guild_id).await.is_some() {
            return;
        }
        let target = match guild_id.parse::<u64>() {
            Ok(g) => self
                .shared
                .http
                .get_channels(serenity::all::GuildId::new(g))
                .await
                .ok()
                .and_then(|chans| {
                    chans.into_iter().find(|c| c.name == "general").map(|c| c.id)
                })
                .unwrap_or(fallback),
            Err(_) => fallback,
        };
        // Adopt a previous post of ours rather than reposting.
        let bot_id = self.shared.bot_id.get().copied().unwrap_or(0);
        if let Ok(msgs) = self.shared.http.get_messages(target, None, Some(50)).await
            && let Some(m) = msgs
                .iter()
                .find(|m| m.author.id.get() == bot_id && m.content.starts_with(CONSENT_MARKER))
        {
            let _ = self
                .shared
                .http
                .edit_message(target, m.id, &json!({ "content": consent_content() }), vec![])
                .await;
            self.record_consent_post(guild_id, target, m.id.to_string()).await;
            tracing::info!(guild = guild_id, "adopted existing consent post");
            return;
        }
        match self
            .shared
            .http
            .send_message(target, vec![], &json!({ "content": consent_content() }))
            .await
        {
            Ok(m) => {
                self.record_consent_post(guild_id, target, m.id.to_string()).await;
                tracing::info!(guild = guild_id, channel = target.to_string(), "posted consent notice");
            }
            Err(e) => tracing::warn!(guild = guild_id, "couldn't post consent notice: {e}"),
        }
    }

    async fn record_consent_post(&self, guild_id: &str, channel: ChannelId, message_id: String) {
        self.shared
            .consent
            .write()
            .await
            .guilds
            .entry(guild_id.to_string())
            .or_default()
            .consent_post = Some(ConsentPost { channel_id: channel.to_string(), message_id });
        self.shared.save_consent().await;
    }
}

pub async fn run(token: String, shared: Arc<Shared>) {
    let intents = GatewayIntents::GUILDS
        | GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::GUILD_MESSAGE_REACTIONS
        | GatewayIntents::DIRECT_MESSAGE_REACTIONS
        | GatewayIntents::MESSAGE_CONTENT;
    loop {
        let handler = Handler { shared: shared.clone() };
        match serenity::Client::builder(&token, intents).event_handler(handler).await {
            Ok(mut client) => {
                // start() reconnects on transient drops; returning is a hard failure.
                if let Err(e) = client.start().await {
                    tracing::error!("gateway stopped: {e}");
                }
            }
            Err(e) => tracing::error!("gateway client build failed: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
        tracing::info!("restarting discord gateway");
    }
}
