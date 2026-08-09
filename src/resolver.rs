use std::time::Duration;

use reqwest::Client;
use tracing::warn;

use crate::config::RouterMode;
use crate::error::RZError;

#[derive(Clone)]
pub struct RzPointResolver {
    client: Client,
    base_url: String,
    mode: RouterMode,
}

impl RzPointResolver {
    pub fn new(rzpoint_addr: &str, mode: RouterMode) -> Result<Self, RZError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| RZError::System(format!("rzpoint client: {e}")))?;

        let base_url = if rzpoint_addr.starts_with("http") {
            rzpoint_addr.trim_end_matches('/').to_string()
        } else {
            format!("http://{}", rzpoint_addr.trim_end_matches('/'))
        };

        Ok(Self {
            client,
            base_url,
            mode,
        })
    }

    pub async fn resolve(&self, hop_id: &str) -> Result<String, RZError> {
        let path = match self.mode {
            RouterMode::Edge => format!("/routers/{}", hop_id),
            RouterMode::Zone => format!("/bridges/{}", hop_id),
        };
        let url = format!("{}{}", self.base_url, path);

        let mut last_err = None;
        for attempt in 0..3 {
            match self.client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let host = resp
                        .text()
                        .await
                        .map_err(|e| RZError::Resolver(e.to_string()))?;
                    let trimmed = host.trim();
                    if trimmed.is_empty() {
                        return Err(RZError::Resolver(format!(
                            "empty response for hop {hop_id}"
                        )));
                    }
                    return Ok(trimmed.to_string());
                }
                Ok(resp) if resp.status().as_u16() == 404 => {
                    return Err(RZError::Resolver(format!("hop {hop_id} not found")));
                }
                Ok(resp) => {
                    let status = resp.status();
                    last_err = Some(RZError::Resolver(format!("HTTP {}", status)));
                }
                Err(e) => {
                    last_err = Some(RZError::Resolver(e.to_string()));
                }
            }
            if attempt < 2 {
                tokio::time::sleep(Duration::from_millis(100 * (attempt + 1))).await;
            }
        }

        warn!(hop_id = %hop_id, "rzpoint resolution failed after retries");
        Err(last_err.unwrap_or_else(|| RZError::Resolver("resolve failed".into())))
    }
}
