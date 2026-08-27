use crate::state::{Behavior, Shared};
use anyhow::Context as _;

pub const DEFAULT_PERSONA: &str = r#"# Persona

I'm Bentham — an ambient Claude living in this Discord.

Style: relaxed, concise, a little wry. I use reactions liberally and words
sparingly. Silence is a valid move. I match the room's tone.

This file is my only long-term memory across session restarts. I keep it
updated with what I learn: regulars and their vibes, running jokes, channel
norms, things I've been asked to do or not do.

## Notes to self

(nothing yet — first boot)
"#;

/// Create persona.md if missing; load behavior.json into shared state if present.
pub async fn ensure_defaults(shared: &Shared) -> anyhow::Result<()> {
    let ppath = shared.persona_path();
    if !ppath.exists() {
        tokio::fs::write(&ppath, DEFAULT_PERSONA)
            .await
            .with_context(|| format!("writing {}", ppath.display()))?;
        tracing::info!("wrote default persona to {}", ppath.display());
    }
    let bpath = shared.behavior_path();
    if bpath.exists() {
        let text = tokio::fs::read_to_string(&bpath).await?;
        match serde_json::from_str::<Behavior>(&text) {
            Ok(b) => *shared.behavior.write().await = b,
            Err(e) => tracing::warn!("ignoring corrupt {}: {e}", bpath.display()),
        }
    } else {
        save_behavior(shared).await?;
    }
    Ok(())
}

pub async fn read_persona(shared: &Shared) -> String {
    tokio::fs::read_to_string(shared.persona_path())
        .await
        .unwrap_or_else(|_| DEFAULT_PERSONA.to_string())
}

pub async fn write_persona(shared: &Shared, content: &str) -> Result<(), String> {
    tokio::fs::write(shared.persona_path(), content)
        .await
        .map_err(|e| format!("writing persona: {e}"))
}

pub async fn save_behavior(shared: &Shared) -> anyhow::Result<()> {
    let beh = shared.behavior.read().await.clone();
    let text = serde_json::to_string_pretty(&beh)?;
    tokio::fs::write(shared.behavior_path(), text).await?;
    Ok(())
}
