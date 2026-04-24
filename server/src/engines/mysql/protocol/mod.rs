pub mod commands;
pub mod connection;
pub mod constants;
pub mod packet;
pub mod tls;
pub mod types;

use crate::engines::traits::Protocol;
use crate::squeal::exec::Executor;
use anyhow::Result;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;

pub struct MysqlProtocol {
    executor: Arc<Executor>,
    tls_config: Option<Arc<tls::TlsConfig>>,
}

impl MysqlProtocol {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self {
            executor,
            tls_config: None,
        }
    }

    pub fn with_tls(executor: Arc<Executor>, cert: &str, key: &str) -> Result<Self> {
        let tls_config = Arc::new(tls::TlsConfig::new(cert, key)?);
        Ok(Self {
            executor,
            tls_config: Some(tls_config),
        })
    }

    pub fn has_tls(&self) -> bool {
        self.tls_config.is_some()
    }
}

impl Protocol for MysqlProtocol {
    fn name(&self) -> &'static str {
        "mysql"
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
            use crate::engines::mysql::protocol::connection::handle_connection;
            use tokio::net::TcpListener;
            use tracing::{error, info};

            let listener = TcpListener::bind(addr).await?;
            info!("MySQL Protocol listening on {}", addr);

            loop {
                let (socket, _) = listener.accept().await?;
                let executor = executor.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(socket, executor).await {
                        error!("MySQL connection error: {}", e);
                    }
                });
            }
        })
    }
}
