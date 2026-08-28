//! The bot's self-editable layer: one persona file and one behavior record
//! per scope (server or DM). A persona is the only memory that survives a
//! session reset.

use crate::prompts;
use crate::state::{Scope, Shared, atomic_write};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// All = any opted-in human message wakes; Mentions = only @mentions and DMs.
/// Serializes as "all"/"mentions" — behaviors.json's exact format.
#[derive(Clone, Copy, PartialEq, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RespondTo {
    #[default]
    All,
    Mentions,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Behavior {
    /// Channel ids that may wake the bot; empty = all inhabited. DMs always wake.
    #[serde(default)]
    pub watched_channels: Vec<String>,
    #[serde(default)]
    pub respond_to: RespondTo,
    /// Wake on a timer even when quiet. 0 = off.
    #[serde(default)]
    pub idle_wake_minutes: u64,
}

fn file(shared: &Shared, scope: Scope) -> PathBuf {
    // Scope displays as a guild id or "dm-<channel id>" — filesystem-safe by type.
    shared.personas_dir().join(format!("{scope}.md"))
}

pub async fn read(shared: &Shared, scope: Scope) -> String {
    tokio::fs::read_to_string(file(shared, scope))
        .await
        .unwrap_or_else(|_| prompts::DEFAULT_PERSONA.to_string())
}

pub async fn write(shared: &Shared, scope: Scope, content: &str) -> Result<(), String> {
    atomic_write(&file(shared, scope), content).map_err(|e| format!("writing persona: {e}"))
}

pub async fn behavior_for(shared: &Shared, scope: Scope) -> Behavior {
    shared.behaviors.read().await.get(&scope).cloned().unwrap_or_default()
}

pub async fn save_behaviors(shared: &Shared) {
    let b = shared.behaviors.read().await.clone();
    let text = serde_json::to_string_pretty(&b).unwrap_or_default();
    if let Err(e) = atomic_write(&shared.behaviors_path(), &text) {
        tracing::warn!("writing behaviors.json: {e}");
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn behaviors_json_byte_compatible() {
        // Mirrors the live behaviors.json shape exactly, bytes and all.
        let text = "{\n  \"1436577999431405709\": {\n    \"watched_channels\": [],\n    \"respond_to\": \"all\",\n    \"idle_wake_minutes\": 0\n  }\n}";
        let map: HashMap<Scope, Behavior> = serde_json::from_str(text).unwrap();
        assert_eq!(map[&Scope::Guild(1436577999431405709)].respond_to, RespondTo::All);
        assert_eq!(serde_json::to_string_pretty(&map).unwrap(), text);
        // DM scopes round-trip through the map key form too.
        let dm: HashMap<Scope, Behavior> =
            serde_json::from_str("{\"dm-775224960289341441\": {\"respond_to\": \"mentions\"}}").unwrap();
        assert_eq!(dm[&Scope::Dm(775224960289341441)].respond_to, RespondTo::Mentions);
        assert!(serde_json::to_string(&dm).unwrap().contains("\"dm-775224960289341441\""));
    }
}
