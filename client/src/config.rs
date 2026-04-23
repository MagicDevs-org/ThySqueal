use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Config {
    #[serde(default)]
    pub connection: ConnectionConfig,
    #[serde(default)]
    pub repl: ReplConfig,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionConfig {
    #[serde(default = "default_host")]
    pub default_host: String,
    #[serde(default = "default_port")]
    pub default_port: u16,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            default_host: default_host(),
            default_port: default_port(),
        }
    }
}

#[allow(dead_code)]
fn default_host() -> String {
    "localhost".to_string()
}

#[allow(dead_code)]
fn default_port() -> u16 {
    8888
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReplConfig {
    #[serde(default = "default_history_size")]
    pub history_size: usize,
    #[serde(default)]
    pub auto_indent: bool,
    #[serde(default = "default_prompt")]
    pub prompt: String,
}

impl Default for ReplConfig {
    fn default() -> Self {
        Self {
            history_size: default_history_size(),
            auto_indent: true,
            prompt: default_prompt(),
        }
    }
}

#[allow(dead_code)]
fn default_history_size() -> usize {
    1000
}

#[allow(dead_code)]
fn default_prompt() -> String {
    "thy> ".to_string()
}

#[allow(dead_code)]
pub fn load_config() -> Config {
    // For now just return default
    Config::default()
}
