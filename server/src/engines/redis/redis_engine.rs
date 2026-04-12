use crate::engines::traits::{Config, DummyConfig, DummyParser, Engine, Parser};

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
}
