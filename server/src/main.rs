mod config;

use std::net::SocketAddr;
use axum::{
    Router,
    routing::get,
};
use tower_http::cors::{CorsLayer, Any};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Clone)]
struct AppState {
    config: config::Config,
}

async fn root() -> &'static str {
    "thy-squeal SQL server"
}

async fn health() -> &'static str {
    "OK"
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    info!("Starting thy-squeal server v{}", env!("CARGO_PKG_VERSION"));

    let config = config::load_config()?;
    info!("Configuration loaded: sql_port={}, http_port={}", 
          config.server.sql_port, config.server.http_port);

    let state = AppState { config: config.clone() };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.server.http_port));
    
    info!("HTTP server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
