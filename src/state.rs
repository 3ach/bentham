//! Shared runtime state, composed from the subsystem types, plus the two
//! cross-cutting primitives: session tokens and memory invalidation.

use crate::buffer::Buffer;
use crate::config::Config;
use crate::consent::Consent;
use crate::persona::Behavior;
use serde::{Deserialize, Serialize};
use serenity::http::Http;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, Notify, RwLock};

pub struct Shared {
    pub cfg: Config,
    pub http: Arc<Http>,
    pub buffer: Buffer,
    pub consent: RwLock<Consent>,
    /// scope -> behavior; missing scope = defaults.
    pub behaviors: RwLock<HashMap<String, Behavior>>,
    /// channel -> claude session bookkeeping.
    pub sessions: std::sync::Mutex<HashMap<String, SessState>>,
    pub tokens: Tokens,
    pub typing: Typing,
    /// Persona scrubs waiting for a maintenance turn (supervisor.rs drains).
    pub pending_scrubs: std::sync::Mutex<Vec<ScrubJob>>,
    /// Bumped whenever transcripts are invalidated: turns that started before
    /// the bump must not re-record their session id.
    pub scrub_gen: AtomicU64,
    /// Serializes consent-post creation so concurrent events can't double-post.
    pub consent_post_lock: Mutex<()>,
    pub bot_id: OnceLock<u64>,
    pub bot_name: OnceLock<String>,
    /// "Something changed" — new message, scrub queued, turn ended.
    pub notify: Notify,
}

impl Shared {
    pub fn new(token: &str, cfg: Config) -> Self {
        Self {
            http: Arc::new(Http::new(token)),
            cfg,
            buffer: Buffer::default(),
            consent: RwLock::new(Consent::default()),
            behaviors: RwLock::new(HashMap::new()),
            sessions: std::sync::Mutex::new(HashMap::new()),
            tokens: Tokens::default(),
            typing: Typing::default(),
            pending_scrubs: std::sync::Mutex::new(Vec::new()),
            scrub_gen: AtomicU64::new(0),
            consent_post_lock: Mutex::new(()),
            bot_id: OnceLock::new(),
            bot_name: OnceLock::new(),
            notify: Notify::new(),
        }
    }

    pub fn personas_dir(&self) -> PathBuf { self.cfg.data_dir.join("personas") }
    pub fn behaviors_path(&self) -> PathBuf { self.cfg.data_dir.join("behaviors.json") }
    pub fn state_path(&self) -> PathBuf { self.cfg.data_dir.join("state.json") }
    pub fn consent_path(&self) -> PathBuf { self.cfg.data_dir.join("consent.json") }
    pub fn mcp_config_path(&self) -> PathBuf { self.cfg.data_dir.join("mcp.json") }

    pub fn save_sessions(&self) {
        let text = serde_json::to_string_pretty(&*self.sessions.lock().unwrap()).unwrap_or_default();
        let _ = std::fs::write(self.state_path(), text);
    }

    /// Memory invalidation: drop every session transcript in a scope, so
    /// nothing said before the change survives into a resumed context. Used
    /// on persona rewrites, opt-outs, and channel leaves.
    pub async fn drop_scope_sessions(&self, scope: &str) {
        if let Some(chan) = scope.strip_prefix("dm-") {
            self.sessions.lock().unwrap().remove(chan);
        } else {
            let chans = crate::consent::guild(self, scope).await.active_channels;
            self.sessions.lock().unwrap().retain(|ch, _| !chans.contains(ch));
        }
        self.scrub_gen.fetch_add(1, Ordering::SeqCst);
        self.save_sessions();
    }
}

/// Per-channel claude session bookkeeping.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct SessState {
    pub session_id: Option<String>,
    pub wakes: u64,
    #[serde(skip)]
    pub failures: u32,
}

/// A queued persona scrub, produced by an opt-out.
#[derive(Clone, PartialEq)]
pub struct ScrubJob {
    pub scope: String,
    pub user_id: String,
    pub user_name: String,
}

/// What a session token resolves to: the one channel and scope a turn may
/// touch. Tools accept nothing else, so isolation holds even against a
/// prompt-injected session.
#[derive(Clone)]
pub struct TurnCtx {
    pub channel_id: String,
    pub scope: String,
    pub is_dm: bool,
    /// Maintenance turns (persona scrubs) may not speak.
    pub maintenance: bool,
}

#[derive(Default)]
pub struct Tokens {
    live: std::sync::Mutex<HashMap<String, TurnCtx>>,
}

impl Tokens {
    pub fn issue(&self, ctx: TurnCtx) -> String {
        use std::io::Read as _;
        let mut b = [0u8; 16];
        if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
            let _ = f.read_exact(&mut b);
        }
        let token: String = b.iter().map(|x| format!("{x:02x}")).collect();
        self.live.lock().unwrap().insert(token.clone(), ctx);
        token
    }

    pub fn resolve(&self, token: &str) -> Option<TurnCtx> {
        self.live.lock().unwrap().get(token).cloned()
    }

    pub fn revoke(&self, token: &str) {
        self.live.lock().unwrap().remove(token);
    }
}

/// Channels with a turn in flight vs. just parked listening; the difference
/// is "actively inferring", shown as a Discord typing indicator.
#[derive(Default)]
pub struct Typing {
    active: std::sync::Mutex<HashSet<String>>,
    waiting: std::sync::Mutex<HashSet<String>>,
}

impl Typing {
    pub fn turn_started(&self, ch: &str) {
        self.active.lock().unwrap().insert(ch.to_string());
    }
    pub fn turn_ended(&self, ch: &str) {
        self.active.lock().unwrap().remove(ch);
    }
    pub fn wait_started(&self, ch: &str) {
        self.waiting.lock().unwrap().insert(ch.to_string());
    }
    pub fn wait_ended(&self, ch: &str) {
        self.waiting.lock().unwrap().remove(ch);
    }
    pub fn inferring(&self) -> Vec<String> {
        let waiting = self.waiting.lock().unwrap();
        self.active.lock().unwrap().iter().filter(|c| !waiting.contains(*c)).cloned().collect()
    }
}
