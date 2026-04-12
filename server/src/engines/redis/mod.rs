pub mod connection;
pub mod redis_engine;
pub mod resp;

#[cfg(test)]
mod tests;

use self::connection::handle_connection;
use crate::squeal::exec::Executor;
use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Redis Protocol Handler
pub struct RedisProtocol {
    executor: Arc<Executor>,
}

impl RedisProtocol {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self { executor }
    }

    pub async fn run(&self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Redis Protocol listening on {}", addr);

        loop {
            let (socket, _) = listener.accept().await?;
            let executor = self.executor.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, executor).await {
                    error!("Redis connection error: {}", e);
                }
            });
        }
    }
}
