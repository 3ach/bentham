//! Shared runtime state, composed from the subsystem types, plus the
//! cross-cutting primitives: scopes, session tokens, and memory invalidation.

use crate::buffer::Buffer;
use crate::config::Config;
use crate::consent::Consent;
use crate::persona::Behavior;
use serde::{Deserialize, Serialize};
use serenity::http::Http;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, Notify, RwLock};

/// Isolation scope: a guild, or one DM channel. Displays/parses as the
/// on-disk string form: "<guild_id>" or "dm-<channel_id>".
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Scope {
    Guild(u64),
    Dm(u64),
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scope::Guild(id) => write!(f, "{id}"),
            Scope::Dm(ch) => write!(f, "dm-{ch}"),
        }
    }
}

impl std::str::FromStr for Scope {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.strip_prefix("dm-") {
            Some(ch) => ch.parse().map(Scope::Dm),
            None => s.parse().map(Scope::Guild),
        }
        .map_err(|_| format!("bad scope: {s:?}"))
    }
}

// Manual serde as the Display/FromStr string, so HashMap<Scope, _> keeps
// behaviors.json's exact key format.
impl serde::Serialize for Scope {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Scope {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
    }
}

pub struct Shared {
    pub cfg: Config,
    pub http: Arc<Http>,
    pub buffer: Buffer,
    pub consent: RwLock<Consent>,
    /// scope -> behavior; missing scope = defaults.
    pub behaviors: RwLock<HashMap<Scope, Behavior>>,
    pub sessions: SessionStore,
    pub tokens: Tokens,
    pub typing: Typing,
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
            sessions: SessionStore::default(),
            tokens: Tokens::default(),
            typing: Typing::default(),
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

    /// Our Discord user id, once the gateway has said hello.
    pub fn me(&self) -> Option<u64> {
        self.bot_id.get().copied()
    }

    pub fn save_sessions(&self) {
        if let Err(e) = atomic_write(&self.state_path(), &self.sessions.snapshot_json()) {
            tracing::warn!("writing state.json: {e}");
        }
    }

    /// Memory invalidation: drop every session transcript in a scope, so
    /// nothing said before the change survives into a resumed context. Used
    /// on persona rewrites, opt-outs, and channel leaves.
    pub async fn drop_scope_sessions(&self, scope: Scope) {
        let ids: Vec<String> = match scope {
            Scope::Dm(chan) => self.sessions.drop_channel(&chan.to_string()).into_iter().collect(),
            Scope::Guild(_) => {
                let chans = crate::consent::guild(self, &scope.to_string()).await.active_channels;
                self.sessions.drop_channels_in(&chans)
            }
        };
        self.save_sessions();
        self.reap_transcripts(&ids);
    }

    /// Best-effort delete of discarded session transcripts. claude stores them
    /// as ~/.claude/projects/<data_dir, non-alphanumerics as '-'>/<id>.jsonl.
    pub fn reap_transcripts(&self, ids: &[String]) {
        if ids.is_empty() {
            return;
        }
        let Ok(home) = std::env::var("HOME") else { return };
        let munged: String = self
            .cfg
            .data_dir
            .to_string_lossy()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        let dir = PathBuf::from(home).join(".claude").join("projects").join(munged);
        for id in ids {
            // Ids come from claude's output (UUIDs); refuse anything that
            // could name a file outside the transcript dir.
            if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
                tracing::warn!("not reaping suspicious session id {id:?}");
                continue;
            }
            let path = dir.join(format!("{id}.jsonl"));
            match std::fs::remove_file(&path) {
                Ok(()) => tracing::debug!("reaped transcript {}", path.display()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!("transcript already gone: {}", path.display());
                }
                Err(e) => tracing::warn!("deleting transcript {}: {e}", path.display()),
            }
        }
    }
}

/// Write <path>.tmp then rename into place, so a crash mid-write can never
/// leave a truncated file where good data was.
pub fn atomic_write(path: &Path, text: &str) -> std::io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)
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
    /// Always Guild — opt-outs only exist in guilds.
    pub scope: Scope,
    pub user_id: String,
    pub user_name: String,
}

#[derive(Default)]
struct SessInner {
    /// channel -> bookkeeping; this map IS state.json, serialized verbatim.
    sessions: HashMap<String, SessState>,
    /// Persona scrubs waiting for a maintenance turn (supervisor.rs drains).
    pending_scrubs: Vec<ScrubJob>,
    /// Bumped whenever transcripts are invalidated: turns that started before
    /// the bump must not re-record their session id.
    scrub_gen: u64,
}

/// Session bookkeeping, scrub queue, and the scrub generation under one lock,
/// so gen-check-then-record is atomic. Methods hand back any session ids they
/// discarded; callers pass those to reap_transcripts.
#[derive(Default)]
pub struct SessionStore {
    inner: std::sync::Mutex<SessInner>,
}

impl SessionStore {
    pub fn load(&self, map: HashMap<String, SessState>) {
        self.inner.lock().unwrap().sessions = map;
    }

    /// (state-or-default, first_time, gen at turn start), in one lock.
    pub fn begin_turn(&self, channel: &str) -> (SessState, bool, u64) {
        let inner = self.inner.lock().unwrap();
        (
            inner.sessions.get(channel).cloned().unwrap_or_default(),
            !inner.sessions.contains_key(channel),
            inner.scrub_gen,
        )
    }

    /// Records the turn's session id (if any) unless a scrub landed mid-turn.
    /// Returns (gen unchanged, session id the caller should reap: the old id
    /// displaced by a fresh turn, or on gen mismatch the unrecorded new one).
    pub fn record_session(
        &self,
        channel: &str,
        id: Option<&str>,
        fresh: bool,
        gen_at_start: u64,
    ) -> (bool, Option<String>) {
        let mut inner = self.inner.lock().unwrap();
        if inner.scrub_gen != gen_at_start {
            // Not recording it anywhere, so hand it back for reaping — a
            // resumed turn's transcript carries the full pre-scrub history.
            return (false, id.map(String::from));
        }
        let mut displaced = None;
        if let Some(id) = id {
            let e = inner.sessions.entry(channel.to_string()).or_default();
            if fresh && e.session_id.as_deref() != Some(id) {
                displaced = e.session_id.take();
            }
            e.session_id = Some(id.to_string());
            e.wakes = if fresh { 1 } else { e.wakes + 1 };
        }
        (true, displaced)
    }

    pub fn clear_failures(&self, channel: &str) {
        self.inner.lock().unwrap().sessions.entry(channel.to_string()).or_default().failures = 0;
    }

    /// Increments; at 3 the session may be poisoned, so the entry resets.
    /// Returns (failures after, entry dropped, session id to reap).
    pub fn record_failure(&self, channel: &str) -> (u32, bool, Option<String>) {
        let mut inner = self.inner.lock().unwrap();
        let e = inner.sessions.entry(channel.to_string()).or_default();
        e.failures += 1;
        if e.failures >= 3 {
            return (0, true, std::mem::take(e).session_id);
        }
        (e.failures, false, None)
    }

    /// Remove one channel + invalidate in-flight turns, as one op.
    pub fn drop_channel(&self, channel: &str) -> Option<String> {
        let mut inner = self.inner.lock().unwrap();
        inner.scrub_gen += 1;
        inner.sessions.remove(channel).and_then(|s| s.session_id)
    }

    /// Remove every channel in `chans` + invalidate, as one op.
    pub fn drop_channels_in(&self, chans: &HashSet<String>) -> Vec<String> {
        let mut inner = self.inner.lock().unwrap();
        inner.scrub_gen += 1;
        let mut ids = Vec::new();
        inner.sessions.retain(|ch, st| {
            if chans.contains(ch) {
                ids.extend(st.session_id.take());
                false
            } else {
                true
            }
        });
        ids
    }

    pub fn queue_scrub(&self, job: ScrubJob) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.pending_scrubs.contains(&job) {
            inner.pending_scrubs.push(job);
        }
    }

    pub fn take_scrubs(&self) -> Vec<ScrubJob> {
        self.inner.lock().unwrap().pending_scrubs.drain(..).collect()
    }

    pub fn requeue_scrubs(&self, jobs: Vec<ScrubJob>) {
        self.inner.lock().unwrap().pending_scrubs.extend(jobs);
    }

    /// Pretty JSON of the sessions map only — state.json's exact format.
    pub fn snapshot_json(&self) -> String {
        serde_json::to_string_pretty(&self.inner.lock().unwrap().sessions).unwrap_or_default()
    }
}

/// What a session token resolves to: the one channel and scope a turn may
/// touch. Tools accept nothing else, so isolation holds even against a
/// prompt-injected session.
#[derive(Clone)]
pub struct TurnCtx {
    pub channel_id: String,
    pub scope: Scope,
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

#[derive(Default)]
struct TypingInner {
    active: HashSet<String>,
    waiting: HashSet<String>,
}

/// Channels with a turn in flight vs. just parked listening; the difference
/// is "actively inferring", shown as a Discord typing indicator.
#[derive(Default)]
pub struct Typing {
    inner: std::sync::Mutex<TypingInner>,
}

impl Typing {
    pub fn turn_started(&self, ch: &str) {
        self.inner.lock().unwrap().active.insert(ch.to_string());
    }
    pub fn turn_ended(&self, ch: &str) {
        self.inner.lock().unwrap().active.remove(ch);
    }
    pub fn wait_started(&self, ch: &str) {
        self.inner.lock().unwrap().waiting.insert(ch.to_string());
    }
    pub fn wait_ended(&self, ch: &str) {
        self.inner.lock().unwrap().waiting.remove(ch);
    }
    /// active − waiting, as one consistent snapshot.
    pub fn inferring(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        inner.active.iter().filter(|c| !inner.waiting.contains(*c)).cloned().collect()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_string_round_trip() {
        for s in ["698380873451700235", "dm-775224960289341441"] {
            assert_eq!(s.parse::<Scope>().unwrap().to_string(), s);
        }
        assert_eq!("1436577999431405709".parse::<Scope>(), Ok(Scope::Guild(1436577999431405709)));
        assert_eq!("dm-42".parse::<Scope>(), Ok(Scope::Dm(42)));
        assert!("dm-x".parse::<Scope>().is_err());
        assert!("".parse::<Scope>().is_err());
        assert!("-3".parse::<Scope>().is_err());
    }
}
