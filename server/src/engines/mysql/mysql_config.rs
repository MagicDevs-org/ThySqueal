use crate::engines::traits::Config;

#[allow(dead_code)]
pub struct CompatibilityConfig {
    pub version: String,
}
#[allow(dead_code)]
pub struct TcpConfig {
    pub addr: String,
}
#[allow(dead_code)]
pub struct UdsConfig {
    pub addr: String,
}

#[allow(dead_code)]
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
    fn parse_config(&mut self, _path: String) {}
}
