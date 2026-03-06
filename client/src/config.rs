use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Config {
    #[serde(default)]
    pub connection: ConnectionConfig,
    #[serde(default)]
    pub repl: ReplConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConnectionConfig {
    #[serde(default = "default_host")]
    pub default_host: String,
    #[serde(default = "default_port")]
    pub default_port: u16,
}

fn default_host() -> String {
    "localhost".to_string()
}

fn default_port() -> u16 {
    3306
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            default_host: default_host(),
            default_port: default_port(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplConfig {
    #[serde(default = "default_history_size")]
    pub history_size: usize,
    #[serde(default)]
    pub auto_indent: bool,
    #[serde(default = "default_prompt")]
    pub prompt: String,
}

fn default_history_size() -> usize {
    1000
}

fn default_prompt() -> String {
    "thy> ".to_string()
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

pub fn load_config() -> Config {
    let config_paths = ["~/.thy-squeal/config.yaml", "~/.thy-squeal/config.yml"];

    for path in &config_paths {
        let expanded = shellexpand::tilde(path);
        if let Ok(contents) = std::fs::read_to_string(expanded.as_ref()) {
            if let Ok(config) = serde_yaml::from_str::<Config>(&contents) {
                return config;
            }
        }
    }

    Config::default()
}
