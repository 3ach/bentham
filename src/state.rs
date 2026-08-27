use crate::config::Config;
use serde::{Deserialize, Serialize};
use serenity::http::Http;
use std::collections::VecDeque;
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
    /// Highest seq handed to Claude via wait_for_messages.
    delivered: AtomicU64,
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
            delivered: AtomicU64::new(0),
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

    /// Undelivered events in watched channels; advances the delivered cursor
    /// past everything currently buffered (unwatched messages are skipped for good).
    pub async fn take_undelivered(&self) -> Vec<MsgEvent> {
        let beh = self.behavior.read().await.clone();
        let buf = self.buf.lock().await;
        let cur = self.delivered.load(Ordering::SeqCst);
        let evs: Vec<MsgEvent> = buf
            .iter()
            .filter(|e| e.seq > cur && Self::watched(&beh, e))
            .cloned()
            .collect();
        if let Some(max) = buf.back().map(|e| e.seq) {
            self.delivered.fetch_max(max, Ordering::SeqCst);
        }
        evs
    }

    /// Is there undelivered activity worth waking Claude for?
    /// Never wakes for other bots (delivered as context, but no wake — avoids bot loops).
    pub async fn has_wakeworthy(&self) -> bool {
        let beh = self.behavior.read().await.clone();
        let buf = self.buf.lock().await;
        let cur = self.delivered.load(Ordering::SeqCst);
        buf.iter().any(|e| {
            e.seq > cur
                && Self::watched(&beh, e)
                && !e.author_is_bot
                && (beh.respond_to == "all" || e.mentions_me || e.is_dm)
        })
    }
}
