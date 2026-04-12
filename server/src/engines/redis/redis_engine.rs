use crate::engines::redis::protocol::RedisProtocol;
use crate::engines::traits::{Config, DummyConfig, DummyParser, Engine, Parser, Protocol};
use crate::squeal::exec::Executor;
use std::sync::Arc;

pub struct RedisEngine;

impl Engine for RedisEngine {
    fn config_key(&self) -> &'static str {
        "redis"
    }

    fn config(&self) -> Box<dyn Config> {
        Box::new(DummyConfig)
    }

    fn parser(&self) -> Box<dyn Parser> {
        Box::new(DummyParser)
    }

    fn protocol(&self, executor: Arc<Executor>) -> Box<dyn Protocol> {
        Box::new(RedisProtocol::new(executor))
    }
}
