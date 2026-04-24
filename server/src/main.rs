mod config;
mod engines;
mod http;
mod squeal;
mod storage;

#[cfg(test)]
mod tests;

use crate::config::Config;
use crate::engines::available_engines;
use crate::engines::traits::Registry;
use crate::squeal::exec::Executor;
use crate::storage::Database;
use crate::storage::persistence::SledPersister;

use clap::Parser;
use futures::future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(short, long, default_value = "thysqueal.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    prepare_tracing();

    let args = Args::parse();

    let mut registry = Registry::new();
    for engine in available_engines() {
        registry.register(engine);
    }
    let engine_keys: Vec<&str> = registry.engines.iter().map(|e| e.config_key()).collect();
    info!("Available engines: {}", engine_keys.join(", "));

    let config = load_config(&args.config)?;
    let db = load_db(config.clone());
    let executor = create_executor(config.clone(), db);

    let mut handles = vec![handle_http(executor.clone(), config.clone())];

    for engine in registry.engines.iter() {
        if let Some(handle) = registry.start_protocols(engine.as_ref(), executor.clone(), &config) {
            handles.push(Some(handle));
        }
    }

    let flat_handles: Vec<JoinHandle<()>> = handles.into_iter().flatten().collect();
    let _ = future::join_all(flat_handles).await;

    Ok(())
}

fn prepare_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");
}

fn load_config(config_path: &str) -> anyhow::Result<Arc<Config>> {
    info!("Starting thysqueal server v{}", env!("CARGO_PKG_VERSION"));
    let cfg = config::load_config(config_path)?;
    let config = Arc::new(cfg);
    info!("Configuration loaded:");
    info!(
        "http.enabled={} port={:?}",
        config.server.http.enabled, config.server.http.port
    );
    info!(
        "mysql.enabled={} port={:?}",
        config.server.mysql.enabled, config.server.mysql.port
    );
    info!(
        "redis.enabled={} port={:?}",
        config.server.redis.enabled, config.server.redis.port
    );
    Ok(config)
}

fn load_db(config: Arc<Config>) -> Arc<RwLock<Database>> {
    let data_dir = config.storage.data_dir.clone();
    let db = if !data_dir.is_empty() {
        info!("Initializing persistence at {}", data_dir);
        let persister = Box::new(SledPersister::new(&data_dir).expect("Failed to open database"));
        Database::with_persister(persister, data_dir).expect("Failed to load database")
    } else {
        Database::new()
    };
    Arc::new(RwLock::new(db))
}

fn create_executor(config: Arc<Config>, db: Arc<RwLock<Database>>) -> Arc<Executor> {
    let data_dir = config.storage.data_dir.clone();
    let executor = Executor::new(db).with_data_dir(data_dir);

    let metrics = executor.metrics.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
            metrics
                .uptime_secs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    });

    Arc::new(executor)
}

// HTTP Server handle
fn handle_http(executor: Arc<Executor>, config: Arc<Config>) -> Option<JoinHandle<()>> {
    if !config.server.http.enabled {
        return None;
    }

    match config.server.http.port {
        None => None,
        Some(port) => {
            let addr = format!("{}:{}", config.server.host, port);
            let http_addr: SocketAddr = addr.parse().expect("Invalid http address");
            let handle = tokio::spawn(async move {
                let app = http::create_app(executor, config);
                info!("HTTP server listening on http://{}", http_addr);
                let listener = match tokio::net::TcpListener::bind(http_addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        error!("Failed to bind HTTP listener: {}", e);
                        return;
                    }
                };
                if let Err(e) = axum::serve(listener, app).await {
                    error!("HTTP server error: {}", e);
                }
            });
            Some(handle)
        }
    }
}
