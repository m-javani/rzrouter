use metrics::{Counter, counter};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::Arc;

#[derive(Debug)]
pub struct RouterMetrics {
    pub connections_opened: Counter,
    pub connections_closed: Counter,
    pub frames_received: Counter,
    pub frames_forwarded: Counter,
    pub bytes_received: Counter,
    pub bytes_sent: Counter,
    pub client_errors: Counter, // parse / protocol errors
    pub unknown_segment: Counter,
    pub network_errors: Counter,
    pub timeouts: Counter,
    pub internal_errors: Counter,
    pub keepalives_sent: Counter,
    pub keepalives_received: Counter,
    pub resyncs: Counter, // how many times we recovered from bad magic
}

impl RouterMetrics {
    pub fn new() -> Self {
        Self {
            connections_opened: counter!("router_connections_opened_total"),
            connections_closed: counter!("router_connections_closed_total"),
            frames_received: counter!("router_frames_received_total"),
            frames_forwarded: counter!("router_frames_forwarded_total"),
            bytes_received: counter!("router_bytes_received_total"),
            bytes_sent: counter!("router_bytes_sent_total"),
            client_errors: counter!("router_client_errors_total"),
            unknown_segment: counter!("router_unknown_segment_total"),
            network_errors: counter!("router_network_errors_total"),
            timeouts: counter!("router_timeouts_total"),
            internal_errors: counter!("router_internal_errors_total"),
            keepalives_sent: counter!("router_keepalives_sent_total"),
            keepalives_received: counter!("router_keepalives_received_total"),
            resyncs: counter!("router_resyncs_total"),
        }
    }
}

pub struct Metrics {
    pub router: RouterMetrics,
    pub prometheus_handle: PrometheusHandle,
}

impl Metrics {
    pub fn new() -> Self {
        let prometheus_handle = PrometheusBuilder::new()
            .install_recorder()
            .expect("Failed to install Prometheus recorder");

        Self {
            router: RouterMetrics::new(),
            prometheus_handle,
        }
    }
}

// Convenience type alias used everywhere
pub type SharedMetrics = Arc<Metrics>;
