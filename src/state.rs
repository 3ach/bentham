use crate::config::Config;
use serde::{Deserialize, Serialize};
use serenity::http::Http;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, Notify, RwLock};

const BUF_CAP: usize = 500;

/// One Discord message, as seen by Claude.
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
}

/// Layer-3 behavior knobs, editable by the bot itself via set_behavior.
#[derive(Clone, Serialize, Deserialize)]
pub struct Behavior {
    /// Channel IDs to watch; empty = all channels the bot can see. DMs are always watched.
    #[serde(default)]
    pub watched_channels: Vec<String>,
    /// "mentions" = wake only for @mentions and DMs; "all" = wake for any human message.
    #[serde(default = "d_respond_to")]
    pub respond_to: String,
    /// Wake on a timer even with no activity. 0 = disabled.
    #[serde(default)]
    pub idle_wake_minutes: u64,
}

fn d_respond_to() -> String { "mentions".into() }

impl Default for Behavior {
    fn default() -> Self {
        Self { watched_channels: vec![], respond_to: d_respond_to(), idle_wake_minutes: 0 }
    }
}

pub struct Shared {
    pub cfg: Config,
    pub http: Arc<Http>,
    pub behavior: RwLock<Behavior>,
    pub bot_id: OnceLock<u64>,
    pub bot_name: OnceLock<String>,
    pub notify: Notify,
    buf: Mutex<VecDeque<MsgEvent>>,
    next_seq: AtomicU64,
    /// Per-channel: highest seq handed to Claude via wait_for_messages.
    delivered: Mutex<HashMap<String, u64>>,
}

impl Shared {
    pub fn new(token: &str, cfg: Config) -> Self {
        Self {
            http: Arc::new(Http::new(token)),
            cfg,
            behavior: RwLock::new(Behavior::default()),
            bot_id: OnceLock::new(),
            bot_name: OnceLock::new(),
            notify: Notify::new(),
            buf: Mutex::new(VecDeque::new()),
            next_seq: AtomicU64::new(0),
            delivered: Mutex::new(HashMap::new()),
        }
    }

    pub fn persona_path(&self) -> PathBuf { self.cfg.data_dir.join("persona.md") }
    pub fn behavior_path(&self) -> PathBuf { self.cfg.data_dir.join("behavior.json") }
    pub fn state_path(&self) -> PathBuf { self.cfg.data_dir.join("state.json") }

    pub async fn push_event(&self, mut ev: MsgEvent) {
        ev.seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        {
            let mut buf = self.buf.lock().await;
            if buf.len() >= BUF_CAP {
                buf.pop_front();
            }
            buf.push_back(ev);
        }
        self.notify.notify_waiters();
    }

    fn watched(beh: &Behavior, ev: &MsgEvent) -> bool {
        ev.is_dm
            || beh.watched_channels.is_empty()
            || beh.watched_channels.contains(&ev.channel_id)
    }

    /// Undelivered events for one channel; advances that channel's cursor.
    pub async fn take_undelivered(&self, channel_id: &str) -> Vec<MsgEvent> {
        let buf = self.buf.lock().await;
        let mut del = self.delivered.lock().await;
        let cur = del.get(channel_id).copied().unwrap_or(0);
        let evs: Vec<MsgEvent> = buf
            .iter()
            .filter(|e| e.channel_id == channel_id && e.seq > cur)
            .cloned()
            .collect();
        if let Some(max) = buf.back().map(|e| e.seq) {
            del.insert(channel_id.to_string(), max);
        }
        evs
    }

    /// Channels with undelivered activity worth waking Claude for, as
    /// (channel_id, display_name). Never wakes for other bots (their messages
    /// are delivered as context, but cause no wake — avoids bot loops).
    pub async fn wakeworthy_channels(&self) -> Vec<(String, String)> {
        let beh = self.behavior.read().await.clone();
        let buf = self.buf.lock().await;
        let del = self.delivered.lock().await;
        let mut out: Vec<(String, String)> = Vec::new();
        for e in buf.iter() {
            if e.seq > del.get(&e.channel_id).copied().unwrap_or(0)
                && Self::watched(&beh, e)
                && !e.author_is_bot
                && (beh.respond_to == "all" || e.mentions_me || e.is_dm)
                && !out.iter().any(|(id, _)| *id == e.channel_id)
            {
                let name = if e.is_dm {
                    format!("your DM with {}", e.author_name)
                } else {
                    match &e.channel_name {
                        Some(n) => format!("#{n}"),
                        None => format!("channel {}", e.channel_id),
                    }
                };
                out.push((e.channel_id.clone(), name));
            }
        }
        out
    }
}
