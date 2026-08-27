use crate::state::{MsgEvent, Shared};
use serenity::all::{Context, EventHandler, GatewayIntents, Message, Reaction, Ready, UserId};
use serenity::async_trait;
use std::sync::Arc;
use std::time::Duration;

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

        // Consent gate 1: dormant channels are invisible until bentham is
        // summoned there by an @mention. (DMs are inherently active.)
        if !is_dm {
            let active = self.shared.consent.read().await.active_channels.contains(&channel_id);
            if !active {
                if !mentions_me {
                    return; // never buffered, never seen
                }
                self.shared.consent.write().await.active_channels.insert(channel_id.clone());
                self.shared.save_consent().await;
                tracing::info!(channel = channel_id, "summoned into new channel");
            }
        }

        // Consent gate 2: humans who haven't opted in get their content
        // redacted at ingest (never stored). Addressing bentham directly is
        // consent for that message; other bots have no privacy interest.
        let opted = self
            .shared
            .consent
            .read()
            .await
            .opted_users
            .contains_key(&msg.author.id.to_string());
        let redacted = !is_dm && !mentions_me && !msg.author.bot && !opted;

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

    /// Reacting to one of bentham's messages = opting in.
    async fn reaction_add(&self, _ctx: Context, r: Reaction) {
        let bot_id = self.shared.bot_id.get().copied().unwrap_or(0);
        let Some(user_id) = r.user_id else { return };
        if bot_id == 0 || user_id.get() == bot_id {
            return;
        }
        let on_own_msg = match r.message_author_id {
            Some(a) => a.get() == bot_id,
            None => matches!(
                self.shared.http.get_message(r.channel_id, r.message_id).await,
                Ok(m) if m.author.id.get() == bot_id
            ),
        };
        if !on_own_msg {
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

    /// Removing a reaction from one of bentham's messages = opting back out.
    async fn reaction_remove(&self, _ctx: Context, r: Reaction) {
        let bot_id = self.shared.bot_id.get().copied().unwrap_or(0);
        let Some(user_id) = r.user_id else { return };
        if bot_id == 0 || user_id.get() == bot_id {
            return;
        }
        // The remove event doesn't carry the message author; fetch to check.
        let on_own_msg = matches!(
            self.shared.http.get_message(r.channel_id, r.message_id).await,
            Ok(m) if m.author.id.get() == bot_id
        );
        if !on_own_msg {
            return;
        }
        if let Some(name) = self.shared.consent.write().await.opted_users.remove(&user_id.to_string()) {
            self.shared.save_consent().await;
            tracing::info!(user = name, "opted out");
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
