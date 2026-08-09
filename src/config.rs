use clap::Parser;
use serde::Deserialize;

use crate::error::RZError;

#[derive(Parser, Debug, Clone, Deserialize)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// Router mode: edge or zone
    #[arg(long, default_value = "edge")]
    pub mode: RouterMode,

    /// Listen host (e.g., "0.0.0.0" or "127.0.0.1")
    #[arg(long, default_value = "0.0.0.0")]
    pub listen_host: String,

    /// TCP port to listen on
    #[arg(long, default_value = "9000")]
    pub tcp_port: u16,

    /// RzID address (e.g., "localhost:8080")
    #[arg(long, default_value = "localhost:8080")]
    pub rzid_addr: String,

    /// RzPoint address (e.g., "localhost:8081")
    #[arg(long, default_value = "localhost:8081")]
    pub rzpoint_addr: String,

    /// Zone ID (required for zone mode)
    #[arg(long, default_value = "")]
    pub zone_id: String,

    /// Router ID (required for zone mode)
    #[arg(long, default_value = "")]
    pub router_id: String,

    /// Maximum number of concurrent TCP connections
    #[arg(long, default_value = "10000")]
    pub max_connections: usize,

    /// Number of connections per hop
    #[arg(long, default_value = "4")]
    pub conn_per_hop: usize,

    /// Hop TCP port
    #[arg(long, default_value = "9000")]
    pub hop_tcp_port: u16,

    /// RzID refresh interval in seconds
    #[arg(long, default_value = "10")]
    pub refresh_interval_secs: u64,

    /// RzID heartbeat interval in seconds
    #[arg(long, default_value = "30")]
    pub heartbeat_interval_secs: u64,

    /// Request timeout in seconds
    #[arg(long, default_value = "30")]
    pub request_timeout_secs: u64,

    /// Number of worker threads
    #[arg(long, default_value = "4")]
    pub worker_threads: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum RouterMode {
    Edge,
    Zone,
}

impl std::str::FromStr for RouterMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "edge" => Ok(RouterMode::Edge),
            "zone" => Ok(RouterMode::Zone),
            _ => Err(format!("unknown mode: {}", s)),
        }
    }
}

impl std::fmt::Display for RouterMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouterMode::Edge => write!(f, "edge"),
            RouterMode::Zone => write!(f, "zone"),
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self, RZError> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| RZError::Config(format!("Failed to read config file {path}: {e}")))?;
        let config: Config = serde_yml::from_str(&contents)
            .map_err(|e| RZError::Config(format!("Failed to parse config: {e}")))?;
        Ok(config)
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.listen_host, self.tcp_port)
    }
}
