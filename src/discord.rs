//! Gateway ingest. The consent filter lives in `message()`: a message either
//! passes both and enters the buffer verbatim, or the handler returns and it
//! never exists anywhere in this program.

use crate::buffer::MsgEvent;
use crate::consent;
use crate::state::{Scope, Shared};
use serenity::all::{Context, EventHandler, GatewayIntents, Guild, Message, Reaction, Ready, UserId};
use serenity::async_trait;
use std::sync::Arc;
use std::time::Duration;

struct Handler {
    shared: Arc<Shared>,
}

#[derive(Debug, PartialEq)]
enum Verdict {
    Drop,
    /// An opted-in @mention in a dormant channel: activate it, then accept.
    SummonAccept,
    Accept,
}

/// Pure form of message()'s two guild consent rules (DMs never get here).
/// Rule 1: dormant channels are invisible until an opted-in person @mentions
/// the bot there. Rule 2: non-opted humans are dropped — their messages,
/// including @mentions, never reach the buffer or inference. Other bots pass
/// (no privacy interest; they never cause wakes).
fn consent_filter(
    mentions_me: bool,
    author_is_bot: bool,
    channel_active: bool,
    author_opted: bool,
) -> Verdict {
    if !channel_active {
        if mentions_me && author_opted { Verdict::SummonAccept } else { Verdict::Drop }
    } else if author_is_bot || author_opted {
        Verdict::Accept
    } else {
        Verdict::Drop
    }
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, _ctx: Context, data: Ready) {
        let _ = self.shared.bot_id.set(data.user.id.get());
        let _ = self.shared.bot_name.set(data.user.name.clone());
        tracing::info!("discord ready as {} ({})", data.user.name, data.user.id);
    }

    async fn message(&self, ctx: Context, msg: Message) {
        let me = self.shared.me();
        if me == Some(msg.author.id.get()) {
            return;
        }
        let mentions_me = me.is_some_and(|id| msg.mentions_user_id(UserId::new(id)));
        let channel_id = msg.channel_id.to_string();
        let author_id = msg.author.id.to_string();

        let scope = match msg.guild_id {
            // DMing the bot is consent; each DM is its own isolation scope.
            None => Scope::Dm(msg.channel_id.get()),
            Some(guild_id) => {
                let gid = guild_id.to_string();
                let g = consent::guild(&self.shared, &gid).await;
                let opted = g.opted_users.contains_key(&author_id);
                let active = g.active_channels.contains(&channel_id);
                let verdict = consent_filter(mentions_me, msg.author.bot, active, opted);
                // Backstop; the post is normally created on guild join. Runs
                // before rule 2, so any message in an inhabited channel —
                // even one about to be dropped — can restore a lost post.
                if active || verdict != Verdict::Drop {
                    consent::ensure_post(&self.shared, &gid, msg.channel_id).await;
                }
                match verdict {
                    Verdict::Drop => return,
                    Verdict::SummonAccept => {
                        let mut c = self.shared.consent.write().await;
                        c.guilds.entry(gid.clone()).or_default().active_channels.insert(channel_id.clone());
                        drop(c);
                        consent::save(&self.shared).await;
                        tracing::info!(channel = channel_id, guild = gid, "summoned into new channel");
                    }
                    Verdict::Accept => {}
                }
                Scope::Guild(guild_id.get())
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
        self.shared.buffer.push(ev);
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
        let me = self.shared.me()?;
        let (gid, user_id) = (r.guild_id?, r.user_id?);
        if user_id.get() == me {
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

/// Discord's typing indicator lasts ~10s; refresh under that while inferring.
pub async fn typing_pulse(s: Arc<Shared>) {
    loop {
        for ch in s.typing.inferring() {
            if let Ok(id) = ch.parse::<u64>() {
                let _ = s.http.broadcast_typing(serenity::all::ChannelId::new(id)).await;
            }
        }
        tokio::time::sleep(Duration::from_secs(8)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{Verdict, consent_filter};

    #[test]
    fn consent_filter_table() {
        use Verdict::*;
        // (mentions_me, author_is_bot, channel_active, author_opted, expect)
        let cases = [
            (false, false, false, false, Drop), // dormant, invisible
            (false, false, false, true, Drop),  // opted, but no summon without mention
            (true, false, false, false, Drop),  // non-opted cannot summon
            (true, false, false, true, SummonAccept), // the only human summon path
            (false, true, false, false, Drop),  // bots can't summon
            (false, true, false, true, Drop),
            (true, true, false, false, Drop),   // bot mention in dormant channel still dropped
            (true, true, false, true, SummonAccept), // faithful: rule 1 checks opted only
            (false, false, true, false, Drop),  // active channel, non-opted human
            (true, false, true, false, Drop),   // non-opted mention is dropped
            (false, false, true, true, Accept),
            (true, false, true, true, Accept),
            (false, true, true, false, Accept), // bots pass rule 2 (context only, never wake)
            (true, true, true, false, Accept),
            (false, true, true, true, Accept),
            (true, true, true, true, Accept),
        ];
        for (i, (mention, bot, active, opted, expect)) in cases.into_iter().enumerate() {
            assert_eq!(consent_filter(mention, bot, active, opted), expect, "case {i}");
        }
    }
}
