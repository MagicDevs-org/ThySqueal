mod config;
mod engines;
mod http;
mod squeal;
mod storage;
#[cfg(test)]
mod tests;

use crate::config::Config;
use crate::engines::mysql::protocol::MySqlProtocol;
use crate::engines::redis::RedisProtocol;
use crate::squeal::exec::Executor;
use crate::storage::Database;
use crate::storage::persistence::SledPersister;

use futures::future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    prepare_tracing();
    let config = load_config()?;
    let db = load_db(config.clone());
    let executor = create_executor(config.clone(), db);

    let option_handles = vec![
        handle_mysql(executor.clone(), config.clone()),
        handle_redis(executor.clone(), config.clone()),
        handle_http(executor.clone(), config.clone()),
    ];

    let handles: Vec<JoinHandle<()>> = option_handles.into_iter().flatten().collect();
    let _ = future::join_all(handles).await;

    Ok(())
}

fn prepare_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");
}

fn load_config() -> anyhow::Result<Arc<Config>> {
    info!("Starting thysqueal server v{}", env!("CARGO_PKG_VERSION"));
    let cfg = config::load_config()?;
    let config = Arc::new(cfg);
    info!("Configuration loaded:");
    info!("http_port={:?}", config.server.http_port);
    info!("sql_port={:?}", config.server.sql_port);
    info!("redis_port={:?}", config.server.redis_port);
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
    Arc::new(Executor::new(db).with_data_dir(data_dir))
}

// HTTP Server handle
fn handle_http(executor: Arc<Executor>, config: Arc<Config>) -> Option<JoinHandle<()>> {
    match config.server.http_port {
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

// MySQL Protocol handle
fn handle_mysql(executor: Arc<Executor>, config: Arc<Config>) -> Option<JoinHandle<()>> {
    match config.server.sql_port {
        None => None,
        Some(port) => {
            let mysql_addr = format!("{}:{}", config.server.host, port);
            let handle = tokio::spawn(async move {
                let protocol = MySqlProtocol::new(executor);
                if let Err(e) = protocol.run(&mysql_addr).await {
                    error!("MySQL protocol error: {}", e);
                }
            });
            Some(handle)
        }
    }
}

// Redis Protocol handle
fn handle_redis(executor: Arc<Executor>, config: Arc<Config>) -> Option<JoinHandle<()>> {
    match config.server.redis_port {
        None => None,
        Some(redis_port) => {
            let redis_addr = format!("{}:{}", config.server.host, redis_port);
            let handle = tokio::spawn(async move {
                let protocol = RedisProtocol::new(executor);
                if let Err(e) = protocol.run(&redis_addr).await {
                    error!("Redis protocol error: {}", e);
                }
            });
            Some(handle)
        }
    }
}
