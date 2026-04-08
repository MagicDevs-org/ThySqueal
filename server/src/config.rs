use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    pub http_port: Option<u16>,
    pub sql_port: Option<u16>,
    pub redis_port: Option<u16>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            http_port: None,
            sql_port: None,
            redis_port: None,
        }
    }
}

fn default_host() -> String {
    "127.0.0.0".to_string()
}

#[allow(unused)]
fn default_sql_port() -> u16 {
    3306
}

#[allow(unused)]
fn default_http_port() -> u16 {
    9200
}

#[allow(unused)]
fn default_redis_port() -> u16 {
    6379
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageConfig {
    #[serde(default = "default_max_memory")]
    pub max_memory_mb: u64,
    #[serde(default = "default_cache_size")]
    pub default_cache_size: u64,
    #[serde(default = "default_eviction")]
    pub default_eviction: String,
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval_sec: u64,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: default_max_memory(),
            default_cache_size: default_cache_size(),
            default_eviction: default_eviction(),
            snapshot_interval_sec: default_snapshot_interval(),
            data_dir: default_data_dir(),
        }
    }
}

fn default_max_memory() -> u64 {
    1024
}

fn default_cache_size() -> u64 {
    1000
}

fn default_eviction() -> String {
    "LRU".to_string()
}

fn default_snapshot_interval() -> u64 {
    300
}

fn default_data_dir() -> String {
    "./data".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SecurityConfig {
    pub auth_enabled: bool,
    pub tls_enabled: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

pub fn load_config() -> anyhow::Result<Config> {
    let config_path = Path::new("thysqueal.yaml");
    if config_path.exists() {
        let content = fs::read_to_string(config_path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        return Ok(config);
    }

    tracing::info!("No config file found, using defaults");
    Ok(Config::default())
}

#[allow(dead_code)]
pub fn default_config() -> Config {
    Config::default()
}
