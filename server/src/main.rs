mod config;
mod http;
mod mysql;
mod redis;
mod sql;
mod squeal;
mod storage;
#[cfg(test)]
mod tests;

use crate::config::Config;
use crate::mysql::MySqlProtocol;
use crate::redis::RedisProtocol;
use crate::sql::Executor;
use crate::storage::Database;
use crate::storage::persistence::SledPersister;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    prepare_tracing();
    let config = load_config()?;
    let db = load_db(config.clone());
    let executor = create_executor(config.clone(), db);
    let _ = run_mysql(executor.clone(), config.clone()).await;
    let _ = run_redis(executor.clone(), config.clone()).await;
    let _ = run_http(executor.clone(), config.clone()).await;

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
    info!("Starting thy-squeal server v{}", env!("CARGO_PKG_VERSION"));
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

// HTTP Server Task
async fn run_http(executor: Arc<Executor>, config: Arc<Config>) -> anyhow::Result<()> {
    let http_addr: SocketAddr =
        format!("{}:{}", config.server.host, config.server.http_port).parse()?;
    let http_handle = tokio::spawn(async move {
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
    let _ = tokio::join!(http_handle);
    Ok(())
}

// MySQL Protocol Task
async fn run_mysql(executor: Arc<Executor>, config: Arc<Config>) -> anyhow::Result<()> {
    let mysql_addr = format!("{}:{}", config.server.host, config.server.sql_port);
    let mysql_handle = tokio::spawn(async move {
        let protocol = MySqlProtocol::new(executor);
        if let Err(e) = protocol.run(&mysql_addr).await {
            error!("MySQL protocol error: {}", e);
        }
    });
    let _ = tokio::join!(mysql_handle);
    Ok(())
}

// Redis Protocol Task
async fn run_redis(executor: Arc<Executor>, config: Arc<Config>) -> anyhow::Result<()> {
    if let Some(redis_port) = config.server.redis_port {
        let redis_addr = format!("{}:{}", config.server.host, redis_port);
        let redis_handle = tokio::spawn(async move {
            let protocol = RedisProtocol::new(executor);
            if let Err(e) = protocol.run(&redis_addr).await {
                error!("Redis protocol error: {}", e);
            }
        });
        let _ = tokio::join!(redis_handle);
    }
    Ok(())
}
