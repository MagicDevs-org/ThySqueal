use crate::engines::traits::{Config, DummyConfig, DummyParser, Parser};

pub trait Engine {
    fn config_key(&self) -> &'static str;

    fn config(&self) -> Box<dyn Config> {
        Box::new(DummyConfig)
    }

    fn parser(&self) -> Box<dyn Parser> {
        Box::new(DummyParser)
    }
}
