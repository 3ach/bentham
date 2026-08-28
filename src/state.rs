use crate::config::Config;
use serde::{Deserialize, Serialize};
use serenity::http::Http;
use std::collections::{HashMap, HashSet, VecDeque};
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
    /// Isolation scope this event belongs to (guild id, or "dm-<channel>").
    #[serde(skip_serializing)]
    pub scope: String,
}

/// Per-scope behavior knobs, editable by the bot itself via set_behavior.
#[derive(Clone, Serialize, Deserialize)]
pub struct Behavior {
    /// Channel IDs to watch within this scope; empty = all. DMs are always watched.
    #[serde(default)]
    pub watched_channels: Vec<String>,
    /// "all" = wake for any opted-in human message (default); "mentions" = only @mentions and DMs.
    #[serde(default = "d_respond_to")]
    pub respond_to: String,
    /// Wake on a timer even with no activity. 0 = disabled.
    #[serde(default)]
    pub idle_wake_minutes: u64,
}

fn d_respond_to() -> String { "all".into() }

impl Default for Behavior {
    fn default() -> Self {
        Self { watched_channels: vec![], respond_to: d_respond_to(), idle_wake_minutes: 0 }
    }
}

/// The one consent post per server: reacting to it is how people opt in.
#[derive(Clone, Serialize, Deserialize)]
pub struct ConsentPost {
    pub channel_id: String,
    pub message_id: String,
}

/// Consent state for one server. Nothing here is shared across servers.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct GuildConsent {
    /// Channels where bentham has been summoned (@mentioned by an opted-in
    /// person). Everything else is dropped at ingest, unseen.
    #[serde(default)]
    pub active_channels: HashSet<String>,
    /// user id -> display name at opt-in time, for THIS server only.
    #[serde(default)]
    pub opted_users: HashMap<String, String>,
    #[serde(default)]
    pub consent_post: Option<ConsentPost>,
    /// Users exempt from reconcile-based opt-out (opted in by the operator
    /// without a reaction on record). A live un-react event still opts them out.
    #[serde(default)]
    pub grandfathered: HashSet<String>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Consent {
    #[serde(default)]
    pub guilds: HashMap<String, GuildConsent>,
}

/// Per-channel Claude session bookkeeping (layer 1).
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct SessState {
    pub session_id: Option<String>,
    pub wakes: u64,
    #[serde(skip)]
    pub failures: u32,
}

/// What a session token resolves to: the one channel and scope that turn may touch.
#[derive(Clone)]
pub struct TurnCtx {
    pub channel_id: String,
    pub scope: String,
    pub is_dm: bool,
    /// Maintenance turns (persona scrubs) may not speak.
    pub maintenance: bool,
}

/// A queued persona scrub, triggered by an opt-out.
#[derive(Clone, PartialEq)]
pub struct ScrubJob {
    pub scope: String,
    pub user_id: String,
    pub user_name: String,
}

/// A channel the dispatcher decided to wake.
#[derive(Clone)]
pub struct WakeTarget {
    pub channel_id: String,
    pub name: String,
    pub is_dm: bool,
    pub scope: String,
}

pub struct Shared {
    pub cfg: Config,
    pub http: Arc<Http>,
    /// scope -> behavior; missing scope = defaults.
    pub behaviors: RwLock<HashMap<String, Behavior>>,
    pub consent: RwLock<Consent>,
    pub sessions: std::sync::Mutex<HashMap<String, SessState>>,
    /// Live session tokens: the capability a turn presents to use the tools.
    tokens: std::sync::Mutex<HashMap<String, TurnCtx>>,
    /// Persona scrubs waiting for a maintenance turn.
    pub pending_scrubs: std::sync::Mutex<Vec<ScrubJob>>,
    /// Channels with a claude turn in flight / parked in wait_for_messages.
    /// Their difference = channels actively inferring (shown as typing).
    pub typing_active: std::sync::Mutex<HashSet<String>>,
    pub typing_waiting: std::sync::Mutex<HashSet<String>>,
    /// Bumped by forget_user / ignore_channel: in-flight turns that started
    /// before the bump must not re-record their session id afterward.
    pub scrub_gen: AtomicU64,
    /// Serializes consent-post creation so concurrent events can't double-post.
    pub consent_post_lock: Mutex<()>,
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
            behaviors: RwLock::new(HashMap::new()),
            consent: RwLock::new(Consent::default()),
            sessions: std::sync::Mutex::new(HashMap::new()),
            tokens: std::sync::Mutex::new(HashMap::new()),
            pending_scrubs: std::sync::Mutex::new(Vec::new()),
            typing_active: std::sync::Mutex::new(HashSet::new()),
            typing_waiting: std::sync::Mutex::new(HashSet::new()),
            scrub_gen: AtomicU64::new(0),
            consent_post_lock: Mutex::new(()),
            bot_id: OnceLock::new(),
            bot_name: OnceLock::new(),
            notify: Notify::new(),
            buf: Mutex::new(VecDeque::new()),
            next_seq: AtomicU64::new(0),
            delivered: Mutex::new(HashMap::new()),
        }
    }

    pub fn personas_dir(&self) -> PathBuf { self.cfg.data_dir.join("personas") }
    pub fn behaviors_path(&self) -> PathBuf { self.cfg.data_dir.join("behaviors.json") }
    pub fn state_path(&self) -> PathBuf { self.cfg.data_dir.join("state.json") }
    pub fn consent_path(&self) -> PathBuf { self.cfg.data_dir.join("consent.json") }

    pub async fn save_consent(&self) {
        let c = self.consent.read().await.clone();
        let text = serde_json::to_string_pretty(&c).unwrap_or_default();
        let _ = tokio::fs::write(self.consent_path(), text).await;
    }

    pub async fn save_behaviors(&self) {
        let b = self.behaviors.read().await.clone();
        let text = serde_json::to_string_pretty(&b).unwrap_or_default();
        let _ = tokio::fs::write(self.behaviors_path(), text).await;
    }

    pub fn save_sessions(&self) {
        let text = serde_json::to_string_pretty(&*self.sessions.lock().unwrap()).unwrap_or_default();
        let _ = std::fs::write(self.state_path(), text);
    }

    pub async fn behavior_for(&self, scope: &str) -> Behavior {
        self.behaviors.read().await.get(scope).cloned().unwrap_or_default()
    }

    /// Channels that should show a typing indicator right now: a turn is
    /// running and it is not just parked listening.
    pub fn channels_inferring(&self) -> Vec<String> {
        let waiting = self.typing_waiting.lock().unwrap();
        self.typing_active
            .lock()
            .unwrap()
            .iter()
            .filter(|c| !waiting.contains(*c))
            .cloned()
            .collect()
    }

    // ---- session tokens ----

    pub fn new_token(&self, ctx: TurnCtx) -> String {
        use std::io::Read as _;
        let mut b = [0u8; 16];
        if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
            let _ = f.read_exact(&mut b);
        }
        let tok: String = b.iter().map(|x| format!("{x:02x}")).collect();
        self.tokens.lock().unwrap().insert(tok.clone(), ctx);
        tok
    }

    pub fn resolve_token(&self, token: &str) -> Option<TurnCtx> {
        self.tokens.lock().unwrap().get(token).cloned()
    }

    pub fn drop_token(&self, token: &str) {
        self.tokens.lock().unwrap().remove(token);
    }

    /// Drop all session transcripts belonging to one scope and bump the
    /// scrub generation so in-flight turns don't re-record them. Used when
    /// what bentham is allowed to remember changes: persona rewrites,
    /// forget_user, opt-outs, leaving a channel.
    pub async fn drop_scope_sessions(&self, scope: &str) {
        if let Some(chan) = scope.strip_prefix("dm-") {
            self.sessions.lock().unwrap().remove(chan);
        } else {
            let chans = self
                .consent
                .read()
                .await
                .guilds
                .get(scope)
                .map(|g| g.active_channels.clone())
                .unwrap_or_default();
            self.sessions.lock().unwrap().retain(|ch, _| !chans.contains(ch));
        }
        self.scrub_gen.fetch_add(1, Ordering::SeqCst);
        self.save_sessions();
    }

    /// Register an opt-in (idempotent). Returns true if newly opted.
    pub async fn opt_in(&self, gid: &str, user_id: &str, name: &str) -> bool {
        let newly = {
            let mut c = self.consent.write().await;
            c.guilds
                .entry(gid.to_string())
                .or_default()
                .opted_users
                .insert(user_id.to_string(), name.to_string())
                .is_none()
        };
        if newly {
            self.save_consent().await;
        }
        newly
    }

    /// The full opt-out pipeline: consent removed, sessions burned, buffer
    /// purged, persona scrub queued. Returns the name if they were opted in.
    pub async fn opt_out(&self, gid: &str, user_id: &str) -> Option<String> {
        let removed = {
            let mut c = self.consent.write().await;
            c.guilds.entry(gid.to_string()).or_default().opted_users.remove(user_id)
        };
        if let Some(name) = &removed {
            self.save_consent().await;
            self.drop_scope_sessions(gid).await;
            self.purge_user(gid, user_id).await;
            {
                let mut q = self.pending_scrubs.lock().unwrap();
                let job = ScrubJob {
                    scope: gid.to_string(),
                    user_id: user_id.to_string(),
                    user_name: name.clone(),
                };
                if !q.contains(&job) {
                    q.push(job);
                }
            }
            self.notify.notify_waiters();
        }
        removed
    }

    // ---- message buffer ----

    pub async fn purge_channel(&self, channel_id: &str) {
        self.buf.lock().await.retain(|e| e.channel_id != channel_id);
    }

    /// Purge one user's messages within one scope only.
    pub async fn purge_user(&self, scope: &str, user_id: &str) {
        self.buf.lock().await.retain(|e| !(e.scope == scope && e.author_id == user_id));
    }

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

    /// Channels with undelivered activity worth waking Claude for. Other
    /// bots never wake (context only — avoids bot loops).
    pub async fn wakeworthy_channels(&self) -> Vec<WakeTarget> {
        let behs = self.behaviors.read().await.clone();
        let buf = self.buf.lock().await;
        let del = self.delivered.lock().await;
        let mut out: Vec<WakeTarget> = Vec::new();
        for e in buf.iter() {
            let beh = behs.get(&e.scope).cloned().unwrap_or_default();
            if e.seq > del.get(&e.channel_id).copied().unwrap_or(0)
                && Self::watched(&beh, e)
                && !e.author_is_bot
                && (beh.respond_to == "all" || e.mentions_me || e.is_dm)
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
}
