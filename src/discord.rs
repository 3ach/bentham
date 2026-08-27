use crate::state::{ConsentPost, MsgEvent, Shared};
use serde_json::json;
use serenity::all::{
    ChannelId, ChannelType, Context, EventHandler, GatewayIntents, Guild, Message, Reaction,
    Ready, UserId,
};
use serenity::async_trait;
use std::sync::Arc;
use std::time::Duration;

const CONSENT_POST: &str = "\u{1F44B} I'm bentham, an AI presence on this server. How privacy works with me:\n\
\u{2022} I can only read messages from people who **opt in** \u{2014} react to **this message** with any emoji to opt in.\n\
\u{2022} Remove your reaction any time to opt back out.\n\
\u{2022} I only inhabit channels where someone @mentions me; everywhere else I see nothing.\n\
\u{2022} Ask me to forget you and I'll scrub you from my memory. Ask me to leave a channel and I'll go.\n\
Messages from anyone who hasn't opted in reach me redacted.";

pub const REDACTED: &str = "[redacted — this person hasn't opted in. They can opt in by \
reacting to any of your messages; do not speculate about what they said]";

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
        let opted = self
            .shared
            .consent
            .read()
            .await
            .opted_users
            .contains_key(&msg.author.id.to_string());

        // Consent gate 1: dormant channels are invisible until bentham is
        // summoned there by an @mention from someone who has opted in —
        // mentions carry no privileges for anyone else. (DMs are inherently
        // active.)
        if !is_dm {
            let active = self.shared.consent.read().await.active_channels.contains(&channel_id);
            if !active {
                if !(mentions_me && opted) {
                    return; // never buffered, never seen
                }
                self.shared.consent.write().await.active_channels.insert(channel_id.clone());
                self.shared.save_consent().await;
                tracing::info!(channel = channel_id, "summoned into new channel");
            }
            // Backstop; the consent post is normally created on guild join.
            if let Some(gid) = msg.guild_id {
                self.ensure_consent_post(gid.to_string(), msg.channel_id).await;
            }
        }

        // Consent gate 2: humans who haven't opted in get their content
        // redacted at ingest (never stored) — even when they @mention bentham.
        // Other bots have no privacy interest. DMing him is consent.
        let redacted = !is_dm && !msg.author.bot && !opted;

        let content = if redacted {
            REDACTED.to_string()
        } else {
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
            author_id: msg.author.id.to_string(),
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
            redacted,
        };
        self.shared.push_event(ev).await;
    }

    /// Fires on join and once per guild at startup: make sure the standing
    /// consent post exists, since opting in via it is the only door into
    /// bentham — even summons require it.
    async fn guild_create(&self, _ctx: Context, guild: Guild, _is_new: Option<bool>) {
        let gid = guild.id.to_string();
        if self.shared.consent.read().await.consent_posts.contains_key(&gid) {
            return;
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
            Some(t) => self.ensure_consent_post(gid, t).await,
            None => tracing::warn!(guild = gid, "no text channel for consent notice"),
        }
    }

    /// Reacting to the server's consent post = opting in.
    async fn reaction_add(&self, _ctx: Context, r: Reaction) {
        let bot_id = self.shared.bot_id.get().copied().unwrap_or(0);
        let (Some(gid), Some(user_id)) = (r.guild_id, r.user_id) else { return };
        if bot_id == 0 || user_id.get() == bot_id {
            return;
        }
        if !self.is_consent_post(&gid.to_string(), r.message_id.get()).await {
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
            .opted_users
            .insert(user_id.to_string(), name.clone());
        self.shared.save_consent().await;
        tracing::info!(user = name, "opted in");
    }

    /// Removing a reaction from the consent post = opting back out.
    async fn reaction_remove(&self, _ctx: Context, r: Reaction) {
        let bot_id = self.shared.bot_id.get().copied().unwrap_or(0);
        let (Some(gid), Some(user_id)) = (r.guild_id, r.user_id) else { return };
        if bot_id == 0 || user_id.get() == bot_id {
            return;
        }
        if !self.is_consent_post(&gid.to_string(), r.message_id.get()).await {
            return;
        }
        // NB: bind the removal result first — an `if let` scrutinee keeps its
        // temporaries (here, the write guard) alive through the success block,
        // which would deadlock against save_consent's read lock.
        let removed = self.shared.consent.write().await.opted_users.remove(&user_id.to_string());
        if let Some(name) = removed {
            self.shared.save_consent().await;
            tracing::info!(user = name, "opted out");
        }
    }
}

impl Handler {
    async fn is_consent_post(&self, guild_id: &str, message_id: u64) -> bool {
        self.shared
            .consent
            .read()
            .await
            .consent_posts
            .get(guild_id)
            .is_some_and(|p| p.message_id == message_id.to_string())
    }

    /// Post the server's one standing consent notice if it doesn't exist yet.
    /// Goes to #general if there is one, else to the channel that triggered
    /// this. Posting is not watching: the target channel stays dormant.
    async fn ensure_consent_post(&self, guild_id: String, fallback: ChannelId) {
        if self.shared.consent.read().await.consent_posts.contains_key(&guild_id) {
            return;
        }
        let _guard = self.shared.consent_post_lock.lock().await;
        if self.shared.consent.read().await.consent_posts.contains_key(&guild_id) {
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
                    chans
                        .into_iter()
                        .find(|c| c.name == "general")
                        .map(|c| c.id)
                })
                .unwrap_or(fallback),
            Err(_) => fallback,
        };
        match self
            .shared
            .http
            .send_message(target, vec![], &json!({ "content": CONSENT_POST }))
            .await
        {
            Ok(m) => {
                self.shared.consent.write().await.consent_posts.insert(
                    guild_id.clone(),
                    ConsentPost { channel_id: target.to_string(), message_id: m.id.to_string() },
                );
                self.shared.save_consent().await;
                tracing::info!(guild = guild_id, channel = target.to_string(), "posted consent notice");
            }
            Err(e) => tracing::warn!(guild = guild_id, "couldn't post consent notice: {e}"),
        }
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
