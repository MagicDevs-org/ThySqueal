use crate::engines::traits::Protocol;
use crate::squeal::exec::Executor;
use anyhow::Result;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;

pub struct RedisProtocol {
    executor: Arc<Executor>,
}

impl RedisProtocol {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self { executor }
    }
}

impl Protocol for RedisProtocol {
    fn name(&self) -> &'static str {
        "redis"
    }

    fn create(executor: Arc<Executor>) -> Self
    where
        Self: Sized,
    {
        Self::new(executor)
    }

    fn run<'a>(&'a self, addr: &'a str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        let executor = self.executor.clone();
        Box::pin(async move {
            use crate::engines::redis::connection::handle_connection;
            use tokio::net::TcpListener;
            use tracing::{error, info};

            let listener = TcpListener::bind(addr).await?;
            info!("Redis Protocol listening on {}", addr);

            loop {
                let (socket, _) = listener.accept().await?;
                let executor = executor.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(socket, executor).await {
                        error!("Redis connection error: {}", e);
                    }
                });
            }
        })
    }
}
