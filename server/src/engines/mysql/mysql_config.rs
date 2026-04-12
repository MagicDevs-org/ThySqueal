use crate::engines::traits::Config;

pub struct CompatibilityConfig {
    pub version: String,
}
pub struct TcpConfig {
    pub addr: String,
}
pub struct UdsConfig {
    pub addr: String,
}

pub struct MysqlConfig {
    pub compatibility: Option<CompatibilityConfig>,
    pub tcp: Option<TcpConfig>,
    pub uds: Option<UdsConfig>,
}

impl MysqlConfig {
    pub fn new() -> Self {
        Self {
            compatibility: None,
            tcp: None,
            uds: None,
        }
    }
}

impl Config for MysqlConfig {
    fn parse_config(&mut self, _path: String) {
        ()
    }
}
