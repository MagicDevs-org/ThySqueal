pub mod mysql;
pub mod redis;
pub mod traits;

use crate::engines::mysql::mysql_engine::MysqlEngine;
use crate::engines::redis::redis_engine::RedisEngine;
use crate::engines::traits::Engine;

pub fn available_engines() -> Vec<Box<dyn Engine>> {
    vec![Box::new(MysqlEngine), Box::new(RedisEngine)]
}
