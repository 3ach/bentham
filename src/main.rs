// bentham: an ambient, consent-gated Claude presence for Discord.
//
//   discord.rs ── gateway events ──[consent gates]──▶ buffer.rs
//   supervisor.rs watches the buffer; per channel, runs one `claude -p` turn
//     prompts.rs = everything a turn is told   tools.rs = everything it can do
//     mcp.rs     = localhost JSON-RPC shell delivering tool calls to tools.rs
//   consent.rs ── who may be seen: opt-in post, reconciler, opt-out pipeline
//   persona.rs ── per-scope self-editable persona/behavior; state.rs ── shared state

mod buffer;
mod config;
mod consent;
mod discord;
mod mcp;
mod persona;
mod prompts;
mod state;
mod supervisor;
mod tools;

use anyhow::Context as _;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "bentham=info,serenity=warn".into()),
        )
        .init();

    let cfg_path = std::env::args().nth(1).unwrap_or_else(|| "config.toml".into());
    let mut cfg = config::Config::load(&cfg_path)?;
    tokio::fs::create_dir_all(&cfg.data_dir).await?;
    cfg.data_dir = cfg.data_dir.canonicalize()?;

    let token = match std::env::var("DISCORD_TOKEN") {
        Ok(t) => t.trim().to_string(),
        Err(_) => tokio::fs::read_to_string(&cfg.discord.token_file)
            .await
            .with_context(|| {
                format!(
                    "no DISCORD_TOKEN env var and couldn't read {}",
                    cfg.discord.token_file.display()
                )
            })?
            .trim()
            .to_string(),
    };

    let shared = Arc::new(state::Shared::new(&token, cfg.clone()));
    consent::load(&shared).await?;
    persona::load(&shared).await?;

    // Tool server for claude sessions, on localhost only.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", cfg.mcp.port))
        .await
        .with_context(|| format!("binding MCP server to port {}", cfg.mcp.port))?;
    tracing::info!("mcp server on http://127.0.0.1:{}/mcp", cfg.mcp.port);
    let app = mcp::router(shared.clone());
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("mcp server died: {e}");
        }
    });

    // Discord gateway (message ingest + consent reactions) and the consent reconciler.
    tokio::spawn(discord::run(token, shared.clone()));
    tokio::spawn(consent::reconcile(shared.clone()));

    // Typing indicator refresh while inferring.
    tokio::spawn(discord::typing_pulse(shared.clone()));

    // The session supervisor, until ctrl-c.
    tokio::select! {
        _ = supervisor::run(shared.clone()) => {}
        _ = tokio::signal::ctrl_c() => tracing::info!("shutting down"),
    }
    Ok(())
}
