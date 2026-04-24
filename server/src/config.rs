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
    pub http: HttpConfig,
    pub mysql: MySqlConfig,
    pub redis: RedisConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            http: HttpConfig::default(),
            mysql: MySqlConfig::default(),
            redis: RedisConfig::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HttpConfig {
    pub enabled: bool,
    pub port: Option<u16>,
    pub host: Option<String>,
    pub path: Option<String>,
    pub tls_enabled: Option<bool>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: Some(8888),
            host: None,
            path: None,
            tls_enabled: Some(false),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MySqlConfig {
    pub enabled: bool,
    pub port: Option<u16>,
    pub host: Option<String>,
    pub path: Option<String>,
    pub version: Option<String>,
    pub tls_enabled: Option<bool>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
}

impl Default for MySqlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: Some(13306),
            host: None,
            path: None,
            version: Some("8.0.0".to_string()),
            tls_enabled: Some(false),
            tls_cert: None,
            tls_key: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RedisConfig {
    pub enabled: bool,
    pub port: Option<u16>,
    pub host: Option<String>,
    pub path: Option<String>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: Some(16379),
            host: None,
            path: None,
        }
    }
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    pub max_memory_mb: Option<u64>,
    pub default_cache_size: Option<u64>,
    pub default_eviction: Option<String>,
    pub snapshot_interval_sec: Option<u64>,
    pub wal_enabled: Option<bool>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            max_memory_mb: Some(1024),
            default_cache_size: Some(1000),
            default_eviction: Some("LRU".to_string()),
            snapshot_interval_sec: Some(300),
            wal_enabled: Some(true),
        }
    }
}

fn default_data_dir() -> String {
    "./data".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SecurityConfig {
    pub auth_enabled: Option<bool>,
    pub tls_enabled: Option<bool>,
    pub default_user: Option<String>,
    pub default_password: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    pub format: Option<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: Some("text".to_string()),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

pub fn load_config(config_path: &str) -> anyhow::Result<Config> {
    let config_path = Path::new(config_path);
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
