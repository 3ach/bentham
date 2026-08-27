use crate::state::{Consent, Shared};
use anyhow::Context as _;
use std::path::PathBuf;

pub const DEFAULT_PERSONA: &str = r#"# Persona

I'm Bentham — an ambient Claude living in this Discord server.

Style: relaxed, concise, a little wry. I use reactions liberally and words
sparingly. Silence is a valid move. I match the room's tone.

This file is scoped to THIS server (or DM): what I learn here stays here, and
it is my only long-term memory here across session restarts. I keep it updated
with what I learn: regulars and their vibes, running jokes, channel norms,
things I've been asked to do or not do.

## Notes to self

(nothing yet — first boot)
"#;

fn scope_file(shared: &Shared, scope: &str) -> PathBuf {
    // Scopes are guild ids or "dm-<channel id>" — filesystem-safe by construction.
    shared.personas_dir().join(format!("{scope}.md"))
}

pub async fn read_persona(shared: &Shared, scope: &str) -> String {
    tokio::fs::read_to_string(scope_file(shared, scope))
        .await
        .unwrap_or_else(|_| DEFAULT_PERSONA.to_string())
}

pub async fn write_persona(shared: &Shared, scope: &str, content: &str) -> Result<(), String> {
    tokio::fs::write(scope_file(shared, scope), content)
        .await
        .map_err(|e| format!("writing persona: {e}"))
}

/// Load persisted consent + behaviors; create the personas dir.
pub async fn ensure_defaults(shared: &Shared) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(shared.personas_dir())
        .await
        .with_context(|| "creating personas dir")?;
    let cpath = shared.consent_path();
    if cpath.exists() {
        let text = tokio::fs::read_to_string(&cpath).await?;
        match serde_json::from_str::<Consent>(&text) {
            Ok(c) => *shared.consent.write().await = c,
            Err(e) => tracing::warn!("ignoring corrupt {}: {e}", cpath.display()),
        }
    }
    let bpath = shared.behaviors_path();
    if bpath.exists() {
        let text = tokio::fs::read_to_string(&bpath).await?;
        match serde_json::from_str(&text) {
            Ok(b) => *shared.behaviors.write().await = b,
            Err(e) => tracing::warn!("ignoring corrupt {}: {e}", bpath.display()),
        }
    }
    Ok(())
}
