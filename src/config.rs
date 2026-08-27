use anyhow::Context as _;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Clone, Deserialize)]
pub struct Config {
    #[serde(default = "d_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub discord: DiscordCfg,
    #[serde(default)]
    pub mcp: McpCfg,
    #[serde(default)]
    pub claude: ClaudeCfg,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let text = if Path::new(path).exists() {
            std::fs::read_to_string(path).with_context(|| format!("reading {path}"))?
        } else {
            tracing::warn!("{path} not found, using defaults");
            String::new()
        };
        toml::from_str(&text).with_context(|| format!("parsing {path}"))
    }
}

#[derive(Clone, Deserialize)]
pub struct DiscordCfg {
    #[serde(default = "d_token_file")]
    pub token_file: PathBuf,
}

impl Default for DiscordCfg {
    fn default() -> Self {
        Self { token_file: d_token_file() }
    }
}

#[derive(Clone, Deserialize)]
pub struct McpCfg {
    #[serde(default = "d_port")]
    pub port: u16,
}

impl Default for McpCfg {
    fn default() -> Self {
        Self { port: d_port() }
    }
}

#[derive(Clone, Deserialize)]
pub struct ClaudeCfg {
    #[serde(default = "d_binary")]
    pub binary: String,
    #[serde(default = "d_model")]
    pub model: String,
    #[serde(default = "d_session_max_wakes")]
    pub session_max_wakes: u64,
    #[serde(default = "d_debounce")]
    pub debounce_seconds: u64,
    #[serde(default = "d_turn_timeout")]
    pub turn_timeout_minutes: u64,
    #[serde(default = "d_disallowed")]
    pub disallowed_tools: Vec<String>,
    #[serde(default)]
    pub extra_args: Vec<String>,
}

impl Default for ClaudeCfg {
    fn default() -> Self {
        toml::from_str("").expect("empty ClaudeCfg")
    }
}

fn d_data_dir() -> PathBuf { "data".into() }
fn d_token_file() -> PathBuf { "data/discord-token".into() }
fn d_port() -> u16 { 43117 }
fn d_binary() -> String { "claude".into() }
fn d_model() -> String { "sonnet".into() }
fn d_session_max_wakes() -> u64 { 50 }
fn d_debounce() -> u64 { 3 }
fn d_turn_timeout() -> u64 { 30 }
fn d_disallowed() -> Vec<String> {
    // No filesystem access at all: the turn's cwd is the data dir, which holds
    // the Discord token. Web tools stay (nothing secret left to exfiltrate).
    ["Bash", "Edit", "Write", "Read", "Glob", "Grep", "LS", "NotebookEdit", "NotebookRead", "Task"]
        .map(String::from)
        .to_vec()
}
