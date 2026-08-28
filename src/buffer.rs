//! In-memory ring of consented messages, with a per-channel delivery cursor.
//! Everything here already passed the ingest gate in discord.rs.

use crate::persona::{Behavior, RespondTo};
use crate::state::Scope;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

const CAP: usize = 500;

/// One Discord message, exactly as a session will see it.
#[derive(Clone, Serialize)]
pub struct MsgEvent {
    pub seq: u64,
    pub message_id: String,
    pub channel_id: String,
    pub channel_name: Option<String>,
    pub guild_name: Option<String>,
    pub is_dm: bool,
    pub author_id: String,
    pub author_name: String,
    pub author_is_bot: bool,
    pub content: String,
    pub timestamp: String,
    pub mentions_me: bool,
    pub reply_to_message_id: Option<String>,
    /// Isolation scope. Internal only — stays off the wire to sessions.
    #[serde(skip_serializing)]
    pub scope: Scope,
}

/// A channel the dispatcher should wake.
#[derive(Clone)]
pub struct WakeTarget {
    pub channel_id: String,
    pub name: String,
    pub is_dm: bool,
    pub scope: Scope,
}

#[derive(Default)]
struct BufferInner {
    ring: VecDeque<MsgEvent>,
    next_seq: u64,
    /// Per channel: highest seq already handed to a session.
    delivered: HashMap<String, u64>,
}

#[derive(Default)]
pub struct Buffer {
    inner: Mutex<BufferInner>,
}

impl Buffer {
    pub fn push(&self, mut ev: MsgEvent) {
        let mut inner = self.inner.lock().unwrap();
        inner.next_seq += 1;
        ev.seq = inner.next_seq;
        if inner.ring.len() >= CAP {
            inner.ring.pop_front();
        }
        inner.ring.push_back(ev);
    }

    /// Undelivered events for one channel; advances that channel's cursor.
    pub fn take_undelivered(&self, channel_id: &str) -> Vec<MsgEvent> {
        let mut inner = self.inner.lock().unwrap();
        let cur = inner.delivered.get(channel_id).copied().unwrap_or(0);
        let evs: Vec<MsgEvent> = inner
            .ring
            .iter()
            .filter(|e| e.channel_id == channel_id && e.seq > cur)
            .cloned()
            .collect();
        let max = inner.ring.back().map(|e| e.seq);
        if let Some(max) = max {
            inner.delivered.insert(channel_id.to_string(), max);
        }
        evs
    }

    /// Channels with undelivered activity that merits waking a session.
    /// Bots never wake (their messages are context only — avoids bot loops).
    pub fn wakeworthy(&self, behaviors: &HashMap<Scope, Behavior>) -> Vec<WakeTarget> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<WakeTarget> = Vec::new();
        for e in inner.ring.iter() {
            let beh = behaviors.get(&e.scope).cloned().unwrap_or_default();
            let undelivered = e.seq > inner.delivered.get(&e.channel_id).copied().unwrap_or(0);
            let watched = e.is_dm
                || beh.watched_channels.is_empty()
                || beh.watched_channels.contains(&e.channel_id);
            let wakes = beh.respond_to == RespondTo::All || e.mentions_me || e.is_dm;
            if undelivered
                && watched
                && wakes
                && !e.author_is_bot
                && !out.iter().any(|t| t.channel_id == e.channel_id)
            {
                let name = if e.is_dm {
                    format!("your DM with {}", e.author_name)
                } else {
                    match &e.channel_name {
                        Some(n) => format!("#{n}"),
                        None => format!("channel {}", e.channel_id),
                    }
                };
                out.push(WakeTarget {
                    channel_id: e.channel_id.clone(),
                    name,
                    is_dm: e.is_dm,
                    scope: e.scope,
                });
            }
        }
        out
    }

    pub fn purge_channel(&self, channel_id: &str) {
        self.inner.lock().unwrap().ring.retain(|e| e.channel_id != channel_id);
    }

    pub fn purge_user(&self, scope: Scope, user_id: &str) {
        self.inner.lock().unwrap().ring.retain(|e| !(e.scope == scope && e.author_id == user_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const G1: Scope = Scope::Guild(1);
    const G2: Scope = Scope::Guild(2);

    fn ev(ch: &str, author: &str, scope: Scope, is_dm: bool, mentions_me: bool, is_bot: bool) -> MsgEvent {
        MsgEvent {
            seq: 0,
            message_id: "m".into(),
            channel_id: ch.into(),
            channel_name: None,
            guild_name: None,
            is_dm,
            author_id: author.into(),
            author_name: author.into(),
            author_is_bot: is_bot,
            content: "hi".into(),
            timestamp: "t".into(),
            mentions_me,
            reply_to_message_id: None,
            scope,
        }
    }

    #[test]
    fn cursor_advance() {
        let b = Buffer::default();
        b.push(ev("1", "a", G1, false, false, false));
        b.push(ev("1", "a", G1, false, false, false));
        let got = b.take_undelivered("1");
        assert_eq!(got.len(), 2);
        assert!(got[0].seq < got[1].seq);
        assert!(b.take_undelivered("1").is_empty());
        b.push(ev("1", "a", G1, false, false, false));
        let got = b.take_undelivered("1");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].seq, 3);
    }

    #[test]
    fn per_channel_isolation() {
        let b = Buffer::default();
        b.push(ev("1", "a", G1, false, false, false));
        b.push(ev("2", "a", G1, false, false, false));
        b.push(ev("1", "a", G1, false, false, false));
        b.push(ev("2", "a", G1, false, false, false));
        let one = b.take_undelivered("1");
        assert_eq!(one.len(), 2);
        assert!(one.iter().all(|e| e.channel_id == "1"));
        // Ch 2's cursor is untouched by ch 1's take.
        assert_eq!(b.take_undelivered("2").len(), 2);
    }

    #[test]
    fn purge_user_scoping() {
        let b = Buffer::default();
        b.push(ev("1", "A", G1, false, false, false));
        b.push(ev("2", "A", G2, false, false, false));
        b.push(ev("1", "B", G1, false, false, false));
        b.purge_user(G1, "A");
        let one = b.take_undelivered("1");
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].author_id, "B");
        // Same user in another scope survives.
        assert_eq!(b.take_undelivered("2").len(), 1);
    }

    #[test]
    fn wakeworthy_bots_never_wake() {
        let b = Buffer::default();
        b.push(ev("1", "bot", G1, false, false, true));
        assert!(b.wakeworthy(&HashMap::new()).is_empty());
    }

    #[test]
    fn wakeworthy_mentions_mode() {
        let behaviors =
            HashMap::from([(G1, Behavior { respond_to: RespondTo::Mentions, ..Default::default() })]);
        let b = Buffer::default();
        b.push(ev("1", "a", G1, false, false, false));
        assert!(b.wakeworthy(&behaviors).is_empty());
        b.push(ev("1", "a", G1, false, true, false));
        assert_eq!(b.wakeworthy(&behaviors).len(), 1);
        // A DM wakes even in mentions mode, without a mention.
        let dm = Scope::Dm(9);
        let behaviors =
            HashMap::from([(dm, Behavior { respond_to: RespondTo::Mentions, ..Default::default() })]);
        let b = Buffer::default();
        b.push(ev("9", "a", dm, true, false, false));
        assert_eq!(b.wakeworthy(&behaviors).len(), 1);
    }

    #[test]
    fn wakeworthy_all_mode_default() {
        // No behavior entry for the scope: defaults wake on any human message.
        let b = Buffer::default();
        b.push(ev("1", "a", G1, false, false, false));
        assert_eq!(b.wakeworthy(&HashMap::new()).len(), 1);
    }

    #[test]
    fn wakeworthy_watched_channels() {
        let behaviors = HashMap::from([(
            G1,
            Behavior { watched_channels: vec!["1".into()], ..Default::default() },
        )]);
        let b = Buffer::default();
        b.push(ev("2", "a", G1, false, false, false));
        assert!(b.wakeworthy(&behaviors).is_empty());
        b.push(ev("1", "a", G1, false, false, false));
        let targets = b.wakeworthy(&behaviors);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].channel_id, "1");
        // DMs bypass the watch list.
        let dm = Scope::Dm(9);
        let behaviors = HashMap::from([(
            dm,
            Behavior { watched_channels: vec!["1".into()], ..Default::default() },
        )]);
        let b = Buffer::default();
        b.push(ev("9", "a", dm, true, false, false));
        assert_eq!(b.wakeworthy(&behaviors).len(), 1);
    }

    #[test]
    fn wakeworthy_delivered_dont_wake() {
        let b = Buffer::default();
        b.push(ev("1", "a", G1, false, false, false));
        b.take_undelivered("1");
        assert!(b.wakeworthy(&HashMap::new()).is_empty());
    }

    #[test]
    fn wakeworthy_dedup() {
        let b = Buffer::default();
        b.push(ev("1", "a", G1, false, false, false));
        b.push(ev("1", "a", G1, false, false, false));
        assert_eq!(b.wakeworthy(&HashMap::new()).len(), 1);
    }
}
