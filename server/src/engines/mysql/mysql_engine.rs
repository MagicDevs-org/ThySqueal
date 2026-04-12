use crate::engines::mysql::mysql_config::MysqlConfig;
use crate::engines::traits::{Config, DummyParser, Engine, Parser};

pub struct MysqlEngine;

impl Engine for MysqlEngine {
    fn config_key(&self) -> &'static str {
        "mysql"
    }

    fn config(&self) -> Box<dyn Config> {
        let mut cfg = MysqlConfig::new();
        cfg.parse_config("/path/to/mysql.yml".to_string());
        Box::new(cfg)
    }

    fn parser(&self) -> Box<dyn Parser> {
        Box::new(DummyParser)
    }
}
