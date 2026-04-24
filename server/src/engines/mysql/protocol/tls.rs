use crate::engines::mysql::error::SqlError;
use std::sync::Arc;

pub struct TlsConfig {
    pub config: Arc<rustls::ServerConfig>,
}

impl TlsConfig {
    pub fn new(_cert_path: &str, _key_path: &str) -> Result<Self, SqlError> {
        Err(SqlError::Runtime(
            "TLS not yet fully implemented - provide cert and key in config".to_string(),
        ))
    }

    pub fn acceptor(&self) -> Arc<rustls::ServerConfig> {
        self.config.clone()
    }
}
