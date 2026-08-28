//! Localhost JSON-RPC shell for the tool server (the subset of MCP that
//! Claude Code speaks). The tools themselves are in tools.rs.

use crate::state::Shared;
use crate::tools;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use std::sync::Arc;

pub fn router(shared: Arc<Shared>) -> Router {
    Router::new()
        .route("/mcp", post(handle).get(async || StatusCode::METHOD_NOT_ALLOWED))
        .with_state(shared)
}

async fn handle(State(s): State<Arc<Shared>>, Json(req): Json<Value>) -> Response {
    let Some(id) = req.get("id").filter(|v| !v.is_null()).cloned() else {
        return StatusCode::ACCEPTED.into_response(); // notification: accept and drop
    };
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let result: Result<Value, (i64, String)> = match method {
        "initialize" => Ok(json!({
            "protocolVersion": req
                .pointer("/params/protocolVersion")
                .cloned()
                .unwrap_or(json!("2025-06-18")),
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "bentham-discord", "version": env!("CARGO_PKG_VERSION") },
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tools::definitions() })),
        "tools/call" => call(&s, req.get("params").cloned().unwrap_or(json!({}))).await,
        _ => Err((-32601, format!("method not found: {method}"))),
    };
    let body = match result {
        Ok(r) => json!({ "jsonrpc": "2.0", "id": id, "result": r }),
        Err((code, message)) => {
            json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
        }
    };
    Json(body).into_response()
}

async fn call(s: &Arc<Shared>, params: Value) -> Result<Value, (i64, String)> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or((-32602, "missing tool name".to_string()))?;
    let args = params.get("arguments").cloned().unwrap_or(json!({}));
    tracing::info!(tool = name, "tool call");
    let out = tools::dispatch(s, name, &args).await;
    Ok(match out {
        Ok(v) => {
            let text = serde_json::to_string_pretty(&v).unwrap_or_else(|_| v.to_string());
            json!({ "content": [{ "type": "text", "text": text }] })
        }
        Err(tools::Error::UnknownTool) => {
            return Err((-32602, format!("unknown tool: {name}")));
        }
        Err(tools::Error::Failed(e)) => {
            json!({ "content": [{ "type": "text", "text": e }], "isError": true })
        }
    })
}
