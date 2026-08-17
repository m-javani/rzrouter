use rzrouter::error::RZError;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::Level;

use rzrouter::config::Config;
use rzrouter::{async_main, init_logging};

use crate::common::client::TestClient;
use crate::common::command::{CommandResponse, get_serialized_command, process_response};
pub struct TestHelper {
    router_addr: String,
    shutdown: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestHelper {
    pub async fn new(mode: &str) -> Self {
        // Setup test logging
        init_logging(Level::DEBUG);

        let config = Self::test_config(mode);
        let router_addr = config.listen_addr();

        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        // Spawn the router
        let handle = tokio::spawn(async move {
            let _ = async_main(config, shutdown_clone).await;
        });

        // Wait for router to be ready
        Self::wait_for_router(&router_addr).await;

        Self {
            router_addr,
            shutdown,
            handle: Some(handle),
        }
    }

    #[allow(unused)]
    pub async fn shutdown(&mut self) {
        self.shutdown.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }

    fn test_config(mode: &str) -> Config {
        use clap::Parser;

        // Build config with test values
        let args = vec![
            "rzrouter",
            "--mode",
            mode,
            "--zone-id",
            "zone1",
            "--router-id",
            "router-zone-0",
            "--rzid-addr",
            "172.20.0.41:8080",
            "--rzpoint-addr",
            "172.20.0.40:9090",
            "--listen-host",
            "127.0.0.1",
            "--tcp-port",
            "9000",
            "--hop-tcp-port",
            "9000",
        ];

        Config::parse_from(args)
    }

    async fn wait_for_router(addr: &str) {
        let max_attempts = 30;
        for attempt in 0..max_attempts {
            match tokio::net::TcpStream::connect(addr).await {
                Ok(_) => {
                    tracing::info!("Router is ready on {}", addr);
                    return;
                }
                Err(_) => {
                    if attempt == max_attempts - 1 {
                        panic!("Router failed to start after {} attempts", max_attempts);
                    }
                    sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    pub async fn create_client(&self) -> TestClient {
        TestClient::connect(&self.router_addr)
            .await
            .expect("Failed to connect to router")
    }

    pub async fn send_command(&self, cmd: &str) -> Result<CommandResponse, RZError> {
        let mut client = self.create_client().await;
        let data = get_serialized_command(cmd);

        let response_payload = client.send_and_receive(&data).await?;
        process_response(cmd, &response_payload)
    }

    #[allow(unused)]
    pub fn router_addr(&self) -> &str {
        &self.router_addr
    }
}

impl Drop for TestHelper {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
