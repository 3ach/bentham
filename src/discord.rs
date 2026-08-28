//! Gateway ingest. The consent gates live in `message()`: a message either
//! passes both and enters the buffer verbatim, or the handler returns and it
//! never exists anywhere in this program.

use crate::buffer::MsgEvent;
use crate::consent;
use crate::state::Shared;
use serenity::all::{Context, EventHandler, GatewayIntents, Guild, Message, Reaction, Ready, UserId};
use serenity::async_trait;
use std::sync::Arc;
use std::time::Duration;

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
        let mentions_me = bot_id != 0 && msg.mentions_user_id(UserId::new(bot_id));
        let channel_id = msg.channel_id.to_string();
        let author_id = msg.author.id.to_string();

        let scope = match msg.guild_id {
            // DMing the bot is consent; each DM is its own isolation scope.
            None => format!("dm-{channel_id}"),
            Some(guild_id) => {
                let gid = guild_id.to_string();
                let g = consent::guild(&self.shared, &gid).await;
                let opted = g.opted_users.contains_key(&author_id);
                // Gate 1: dormant channels are invisible until an opted-in
                // person @mentions the bot there.
                if !g.active_channels.contains(&channel_id) {
                    if !(mentions_me && opted) {
                        return;
                    }
                    let mut c = self.shared.consent.write().await;
                    c.guilds.entry(gid.clone()).or_default().active_channels.insert(channel_id.clone());
                    drop(c);
                    consent::save(&self.shared).await;
                    tracing::info!(channel = channel_id, guild = gid, "summoned into new channel");
                }
                // Backstop; the post is normally created on guild join.
                consent::ensure_post(&self.shared, &gid, msg.channel_id).await;
                // Gate 2: non-opted humans are dropped here — their messages,
                // including @mentions, never reach the buffer or inference.
                // Other bots pass (no privacy interest; they never cause wakes).
                if !msg.author.bot && !opted {
                    return;
                }
                gid
            }
        };

        let mut content = msg.content.clone();
        for a in &msg.attachments {
            content.push_str(&format!("\n[attachment: {}]", a.url));
        }

        let ev = MsgEvent {
            seq: 0, // assigned by the buffer
            message_id: msg.id.to_string(),
            channel_name: msg.channel_id.name(&ctx).await.ok(),
            guild_name: msg.guild_id.and_then(|g| g.name(&ctx.cache)),
            is_dm: msg.guild_id.is_none(),
            channel_id,
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
                .or_else(|| msg.message_reference.as_ref().and_then(|r| r.message_id).map(|i| i.to_string())),
            scope,
        };
        self.shared.buffer.push(ev).await;
        self.shared.notify.notify_waiters();
    }

    async fn guild_create(&self, _ctx: Context, guild: Guild, _is_new: Option<bool>) {
        consent::refresh_post(&self.shared, &guild).await;
    }

    /// Reacting to the server's consent post = opting in (this server only).
    async fn reaction_add(&self, _ctx: Context, r: Reaction) {
        let Some((gid, user_id)) = self.consent_post_reaction(&r).await else { return };
        let name = match &r.member {
            Some(m) => m.user.name.clone(),
            None => match self.shared.http.get_user(user_id).await {
                Ok(u) => u.name,
                Err(_) => user_id.to_string(),
            },
        };
        if consent::opt_in(&self.shared, &gid, &user_id.to_string(), &name).await {
            tracing::info!(user = name, guild = gid, "opted in");
        }
    }

    /// Removing that reaction = opting out, with the full scrub pipeline.
    async fn reaction_remove(&self, _ctx: Context, r: Reaction) {
        let Some((gid, user_id)) = self.consent_post_reaction(&r).await else { return };
        if let Some(name) = consent::opt_out(&self.shared, &gid, &user_id.to_string()).await {
            tracing::info!(user = name, guild = gid, "opted out; sessions dropped, scrub queued");
        }
    }
}

impl Handler {
    /// Some(guild, user) iff this reaction is a non-bot user acting on the
    /// guild's consent post.
    async fn consent_post_reaction(&self, r: &Reaction) -> Option<(String, UserId)> {
        let bot_id = self.shared.bot_id.get().copied().unwrap_or(0);
        let (gid, user_id) = (r.guild_id?, r.user_id?);
        if bot_id == 0 || user_id.get() == bot_id {
            return None;
        }
        let gid = gid.to_string();
        consent::is_post(&self.shared, &gid, r.message_id.get())
            .await
            .then_some((gid, user_id))
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
