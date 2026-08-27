mod config;
mod discord;
mod mcp;
mod persona;
mod state;
mod supervisor;

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
    let cfg = config::Config::load(&cfg_path)?;
    tokio::fs::create_dir_all(&cfg.data_dir).await?;

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
    persona::ensure_defaults(&shared).await?;

    // Layer 2: MCP server (Discord tools + self-amendment tools) on localhost.
    let app = mcp::router(shared.clone());
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], cfg.mcp.port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding MCP server to {addr}"))?;
    tracing::info!("mcp server on http://{addr}/mcp");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("mcp server died: {e}");
        }
    });

    // Layer 2: Discord gateway feeding the message buffer.
    tokio::spawn(discord::run(token, shared.clone()));

    // Layer 1: Claude session supervisor. Runs until ctrl-c.
    tokio::select! {
        _ = supervisor::run(shared.clone()) => {}
        _ = tokio::signal::ctrl_c() => tracing::info!("shutting down"),
    }
    Ok(())
}
