use clap::Parser;
use serde::Deserialize;

use crate::error::RZError;

#[derive(Parser, Debug, Clone)]
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
    #[arg(long, required = true)]
    pub rzid_addr: String,

    /// RzPoint address (e.g., "localhost:8081")
    #[arg(long, required = true)]
    pub rzpoint_addr: String,

    /// Zone ID (REQUIRED for zone mode)
    #[arg(long)]
    pub zone_id: Option<String>,

    /// Router ID (REQUIRED for zone mode)
    #[arg(long)]
    pub router_id: Option<String>,

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
    pub fn parse() -> Result<Self, RZError> {
        let config = <Self as Parser>::parse();

        // Validation: zone mode requires zone_id and router_id
        if config.mode == RouterMode::Zone {
            if config.zone_id.is_none() || config.zone_id.as_ref().unwrap().is_empty() {
                return Err(RZError::Config("zone_id is required for zone mode".into()));
            }
            if config.router_id.is_none() || config.router_id.as_ref().unwrap().is_empty() {
                return Err(RZError::Config(
                    "router_id is required for zone mode".into(),
                ));
            }
        }

        Ok(config)
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.listen_host, self.tcp_port)
    }

    pub fn zone_id(&self) -> &str {
        self.zone_id.as_deref().unwrap_or("")
    }

    pub fn router_id(&self) -> &str {
        self.router_id.as_deref().unwrap_or("")
    }
}
