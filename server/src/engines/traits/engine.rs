use crate::engines::traits::{Config, DummyConfig, DummyParser, Parser, Protocol};
use crate::squeal::exec::Executor;
use std::sync::Arc;

#[allow(dead_code)]
pub trait Engine: Send + Sync {
    fn config_key(&self) -> &'static str;

    fn config(&self) -> Box<dyn Config> {
        Box::new(DummyConfig)
    }

    fn parser(&self) -> Box<dyn Parser> {
        Box::new(DummyParser)
    }

    fn protocol(&self, executor: Arc<Executor>) -> Box<dyn Protocol>;
}
