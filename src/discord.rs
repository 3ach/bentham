use crate::state::{MsgEvent, Shared};
use serenity::all::{Context, EventHandler, GatewayIntents, Message, Ready, UserId};
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

        let mut content = msg.content.clone();
        for a in &msg.attachments {
            content.push_str(&format!("\n[attachment: {}]", a.url));
        }

        let channel_name = msg.channel_id.name(&ctx).await.ok();
        let guild_name = msg.guild_id.and_then(|g| g.name(&ctx.cache));
        let mentions_me = bot_id != 0 && msg.mentions_user_id(UserId::new(bot_id));

        let ev = MsgEvent {
            seq: 0, // assigned by push_event
            message_id: msg.id.to_string(),
            channel_id: msg.channel_id.to_string(),
            channel_name,
            guild_name,
            is_dm: msg.guild_id.is_none(),
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
        };
        self.shared.push_event(ev).await;
    }
}

pub async fn run(token: String, shared: Arc<Shared>) {
    let intents = GatewayIntents::GUILDS
        | GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
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
