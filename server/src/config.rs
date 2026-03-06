use serde::{Deserialize, Serialize};
use std::fs;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    ReadError(#[from] std::io::Error),
    #[error("Failed to parse YAML: {0}")]
    ParseError(#[from] serde_yaml::Error),
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_sql_port")]
    pub sql_port: u16,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_sql_port() -> u16 {
    3306
}

fn default_http_port() -> u16 {
    9200
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            sql_port: default_sql_port(),
            http_port: default_http_port(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    #[serde(default = "default_max_memory_mb")]
    pub max_memory_mb: usize,
    #[serde(default = "default_cache_size")]
    pub default_cache_size: usize,
    #[serde(default = "default_eviction")]
    pub default_eviction: String,
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval_sec: u64,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
}

fn default_max_memory_mb() -> usize {
    4096
}

fn default_cache_size() -> usize {
    10000
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

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: default_max_memory_mb(),
            default_cache_size: default_cache_size(),
            default_eviction: default_eviction(),
            snapshot_interval_sec: default_snapshot_interval(),
            data_dir: default_data_dir(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityConfig {
    #[serde(default)]
    pub auth_enabled: bool,
    #[serde(default)]
    pub tls_enabled: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            auth_enabled: false,
            tls_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

pub fn load_config() -> Result<Config, ConfigError> {
    let config_paths = [
        "thy-squeal.yaml",
        "thy-squeal.yml",
        "config.yaml",
        "config.yml",
    ];

    for path in &config_paths {
        if let Ok(contents) = fs::read_to_string(path) {
            tracing::info!("Loading config from {}", path);
            return Ok(serde_yaml::from_str(&contents)?);
        }
    }

    tracing::info!("No config file found, using defaults");
    Ok(Config::default())
}

pub fn default_config() -> Config {
    Config::default()
}
