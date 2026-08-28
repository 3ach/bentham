//! In-memory ring of consented messages, with a per-channel delivery cursor.
//! Everything here already passed the ingest gate in discord.rs.

use crate::persona::Behavior;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

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
    /// Isolation scope (guild id, or "dm-<channel>"). Internal only.
    #[serde(skip_serializing)]
    pub scope: String,
}

/// A channel the dispatcher should wake.
#[derive(Clone)]
pub struct WakeTarget {
    pub channel_id: String,
    pub name: String,
    pub is_dm: bool,
    pub scope: String,
}

#[derive(Default)]
pub struct Buffer {
    ring: Mutex<VecDeque<MsgEvent>>,
    next_seq: AtomicU64,
    /// Per channel: highest seq already handed to a session.
    delivered: Mutex<HashMap<String, u64>>,
}

impl Buffer {
    pub async fn push(&self, mut ev: MsgEvent) {
        ev.seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let mut ring = self.ring.lock().await;
        if ring.len() >= CAP {
            ring.pop_front();
        }
        ring.push_back(ev);
    }

    /// Undelivered events for one channel; advances that channel's cursor.
    pub async fn take_undelivered(&self, channel_id: &str) -> Vec<MsgEvent> {
        let ring = self.ring.lock().await;
        let mut del = self.delivered.lock().await;
        let cur = del.get(channel_id).copied().unwrap_or(0);
        let evs: Vec<MsgEvent> = ring
            .iter()
            .filter(|e| e.channel_id == channel_id && e.seq > cur)
            .cloned()
            .collect();
        if let Some(max) = ring.back().map(|e| e.seq) {
            del.insert(channel_id.to_string(), max);
        }
        evs
    }

    /// Channels with undelivered activity that merits waking a session.
    /// Bots never wake (their messages are context only — avoids bot loops).
    pub async fn wakeworthy(&self, behaviors: &HashMap<String, Behavior>) -> Vec<WakeTarget> {
        let ring = self.ring.lock().await;
        let del = self.delivered.lock().await;
        let mut out: Vec<WakeTarget> = Vec::new();
        for e in ring.iter() {
            let beh = behaviors.get(&e.scope).cloned().unwrap_or_default();
            let undelivered = e.seq > del.get(&e.channel_id).copied().unwrap_or(0);
            let watched = e.is_dm
                || beh.watched_channels.is_empty()
                || beh.watched_channels.contains(&e.channel_id);
            let wakes = beh.respond_to == "all" || e.mentions_me || e.is_dm;
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
                    scope: e.scope.clone(),
                });
            }
        }
        out
    }

    pub async fn purge_channel(&self, channel_id: &str) {
        self.ring.lock().await.retain(|e| e.channel_id != channel_id);
    }

    pub async fn purge_user(&self, scope: &str, user_id: &str) {
        self.ring.lock().await.retain(|e| !(e.scope == scope && e.author_id == user_id));
    }
}
