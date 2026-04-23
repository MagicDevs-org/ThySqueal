use crate::config::Config;
use crate::engines::traits::Engine;
use crate::squeal::exec::Executor;
use std::sync::Arc;

pub struct Registry {
    pub engines: Vec<Box<dyn Engine>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            engines: Vec::new(),
        }
    }

    pub fn register(&mut self, engine: Box<dyn Engine>) {
        self.engines.push(engine);
    }

    #[allow(dead_code)]
    pub fn get_engine(&self, key: &str) -> Option<&dyn Engine> {
        self.engines
            .iter()
            .find(|e| e.config_key() == key)
            .map(|e| e.as_ref())
    }

    pub fn get_port(&self, engine: &dyn Engine, config: &Config) -> Option<u16> {
        let key = engine.config_key();
        let enabled = match key {
            "mysql" => config.server.mysql.enabled,
            "redis" => config.server.redis.enabled,
            _ => false,
        };
        
        if !enabled {
            return None;
        }
        
        match key {
            "mysql" => config.server.mysql.port,
            "redis" => config.server.redis.port,
            _ => None,
        }
    }

    pub fn start_protocols(
        &self,
        engine: &dyn Engine,
        executor: Arc<Executor>,
        config: &Config,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let port = self.get_port(engine, config)?;

        let addr = format!("{}:{}", config.server.host, port);
        let protocol = engine.protocol(executor);
        let name = protocol.name().to_string();

        Some(tokio::spawn(async move {
            if let Err(e) = protocol.run(&addr).await {
                tracing::error!("{} protocol error: {}", name, e);
            }
        }))
    }
}
