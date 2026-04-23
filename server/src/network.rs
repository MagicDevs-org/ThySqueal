use std::net::SocketAddr;
use std::path::PathBuf;
use crate::config::{MySqlConfig, RedisConfig, HttpConfig};

#[derive(Debug, Clone)]
pub enum NetworkAddr {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

impl NetworkAddr {
    pub fn tcp(host: &str, port: u16) -> Self {
        let addr = format!("{}:{}", host, port);
        NetworkAddr::Tcp(addr.parse().expect("Invalid socket address"))
    }
    
    pub fn unix(path: &str) -> Self {
        NetworkAddr::Unix(PathBuf::from(path))
    }
}

pub fn parse_socket_addr(host: &str, port: u16) -> SocketAddr {
    let addr = format!("{}:{}", host, port);
    addr.parse().expect("Invalid socket address")
}

pub fn get_listen_addr(config: &MySqlConfig, host: &str) -> Option<NetworkAddr> {
    if !config.enabled {
        return None;
    }
    
    if let Some(ref path) = config.path {
        return Some(NetworkAddr::unix(path));
    }
    
    config.port.map(|port| NetworkAddr::tcp(host, port))
}

pub fn get_listen_addr_redis(config: &RedisConfig, host: &str) -> Option<NetworkAddr> {
    if !config.enabled {
        return None;
    }
    
    if let Some(ref path) = config.path {
        return Some(NetworkAddr::unix(path));
    }
    
    config.port.map(|port| NetworkAddr::tcp(host, port))
}

pub fn get_listen_addr_http(config: &HttpConfig, host: &str) -> Option<NetworkAddr> {
    if !config.enabled {
        return None;
    }
    
    if let Some(ref path) = config.path {
        return Some(NetworkAddr::unix(path));
    }
    
    config.port.map(|port| NetworkAddr::tcp(host, port))
}