//! The bot's self-editable layer: one persona file and one behavior record
//! per scope (server or DM). A persona is the only memory that survives a
//! session reset.

use crate::prompts;
use crate::state::Shared;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Serialize, Deserialize)]
pub struct Behavior {
    /// Channel ids that may wake the bot; empty = all inhabited. DMs always wake.
    #[serde(default)]
    pub watched_channels: Vec<String>,
    /// "all" = any opted-in human message wakes; "mentions" = only @mentions and DMs.
    #[serde(default = "d_respond_to")]
    pub respond_to: String,
    /// Wake on a timer even when quiet. 0 = off.
    #[serde(default)]
    pub idle_wake_minutes: u64,
}

fn d_respond_to() -> String { "all".into() }

impl Default for Behavior {
    fn default() -> Self {
        Self { watched_channels: vec![], respond_to: d_respond_to(), idle_wake_minutes: 0 }
    }
}

fn file(shared: &Shared, scope: &str) -> PathBuf {
    // Scopes are guild ids or "dm-<channel id>" — filesystem-safe by construction.
    shared.personas_dir().join(format!("{scope}.md"))
}

pub async fn read(shared: &Shared, scope: &str) -> String {
    tokio::fs::read_to_string(file(shared, scope))
        .await
        .unwrap_or_else(|_| prompts::DEFAULT_PERSONA.to_string())
}

pub async fn write(shared: &Shared, scope: &str, content: &str) -> Result<(), String> {
    tokio::fs::write(file(shared, scope), content)
        .await
        .map_err(|e| format!("writing persona: {e}"))
}

pub async fn behavior_for(shared: &Shared, scope: &str) -> Behavior {
    shared.behaviors.read().await.get(scope).cloned().unwrap_or_default()
}

pub async fn save_behaviors(shared: &Shared) {
    let b = shared.behaviors.read().await.clone();
    let text = serde_json::to_string_pretty(&b).unwrap_or_default();
    let _ = tokio::fs::write(shared.behaviors_path(), text).await;
}

pub async fn load(shared: &Shared) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(shared.personas_dir()).await?;
    let path = shared.behaviors_path();
    if path.exists() {
        match serde_json::from_str(&tokio::fs::read_to_string(&path).await?) {
            Ok(b) => *shared.behaviors.write().await = b,
            Err(e) => tracing::warn!("ignoring corrupt {}: {e}", path.display()),
        }
    }
    Ok(())
}
