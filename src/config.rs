// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzrouter.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use crate::error::RZError;
use clap::Parser;
use serde::Deserialize;
use std::time::Duration;

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

    /// HTTP API listen host (for /metrics and /health)
    #[arg(long, default_value = "0.0.0.0")]
    pub api_listening_addr: String,

    /// HTTP API port (for /metrics and /health)
    #[arg(long, default_value = "9100")]
    pub api_port: u16,

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

    /// RzID heartbeat interval in seconds (for the ID service itself)
    #[arg(long, default_value = "30")]
    pub heartbeat_interval_secs: u64,

    /// Request timeout in seconds (used by the forwarder)
    #[arg(long, default_value = "30")]
    pub request_timeout_secs: u64,

    /// Number of worker threads
    #[arg(long, default_value = "4")]
    pub worker_threads: usize,

    // ------------------------------------------------------------------
    // New robustness / connection management fields
    // ------------------------------------------------------------------
    /// Application-level keepalive interval in seconds.
    /// Router will send a `__keepalive__` frame when the connection has been idle
    /// for this long. 0 disables application keepalives.
    #[arg(long, default_value = "15")]
    pub app_keepalive_secs: u64,

    /// Maximum time allowed to complete a single frame (seconds).
    /// Protects against slowloris / partial-frame DoS.
    /// After this many seconds without a complete frame the connection is closed.
    #[arg(long, default_value = "20")]
    pub frame_timeout_secs: u64,

    /// Absolute idle timeout (seconds). Connection is closed if no activity
    /// (data or keepalive) is seen for this long.
    #[arg(long, default_value = "90")]
    pub idle_timeout_secs: u64,

    /// Maximum size of the receive buffer per connection (bytes).
    /// Protects against memory exhaustion from malicious clients.
    /// Recommended: 256 KiB – 1 MiB for your workload.
    #[arg(long, default_value = "262144")] // 256 KiB
    pub max_buffer_size: usize,

    /// Maximum allowed size of a single frame (bytes).
    /// Frames larger than this are rejected / cause connection drop.
    /// Your payloads are <200 B, so 8–16 KiB is already very generous.
    #[arg(long, default_value = "16384")] // 16 KiB
    pub max_frame_size: usize,
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

        // Basic sanity checks on the new fields
        if config.max_frame_size > config.max_buffer_size {
            return Err(RZError::Config(
                "max_frame_size cannot be larger than max_buffer_size".into(),
            ));
        }
        if config.frame_timeout_secs == 0 {
            return Err(RZError::Config("frame_timeout_secs must be > 0".into()));
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

    // ------------------------------------------------------------------
    // Convenience Duration helpers (used by tcp_server)
    // ------------------------------------------------------------------

    #[inline]
    pub fn app_keepalive_interval(&self) -> Duration {
        Duration::from_secs(self.app_keepalive_secs)
    }

    #[inline]
    pub fn frame_timeout(&self) -> Duration {
        Duration::from_secs(self.frame_timeout_secs)
    }

    #[inline]
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_timeout_secs)
    }

    #[inline]
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }

    #[inline]
    pub fn api_listen_addr(&self) -> String {
        format!("{}:{}", self.api_listening_addr, self.api_port)
    }
}
