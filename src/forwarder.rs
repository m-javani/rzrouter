use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use tracing::debug;

use crate::error::{ForwardError, RZError};
use crate::hop_manager::HopManager;
use crate::routing_state::RoutingSnapshot;

/// The forwarder handles the hot path: route → hop → connection → send → response.
pub struct Forwarder {
    routing: Arc<ArcSwap<RoutingSnapshot>>,
    hops: Arc<HopManager>,
    max_retries: usize,
}

impl Forwarder {
    pub fn new(routing: Arc<ArcSwap<RoutingSnapshot>>, hops: Arc<HopManager>) -> Self {
        Self {
            routing,
            hops,
            max_retries: 3,
        }
    }

    /// Forward a frame to the appropriate hop.
    ///
    /// Returns:
    /// - Ok(Bytes) → Response to send back to client
    /// - Err(ForwardError::UnknownSegment) → Send error response to client
    /// - Err(ForwardError::NetworkError) → Send error response to client
    /// - Err(ForwardError::Timeout) → Drop, client will timeout
    /// - Err(ForwardError::Internal) → Drop, client will timeout
    pub async fn forward(
        &self,
        mut frame: BytesMut,
        segment: String,
        clrid_offset: usize,
        original_clrid: u32,
        is_write: bool,
    ) -> Result<Bytes, ForwardError> {
        // Track whether we've successfully sent the request
        let mut request_sent = false;

        for attempt in 0..self.max_retries {
            // 1. Get routing snapshot
            let snapshot = self.routing.load();

            // 2. Get hop IDs for this segment
            let hop_ids = snapshot.lookup(&segment);
            if hop_ids.is_empty() {
                return Err(ForwardError::UnknownSegment(segment.clone()));
            }

            // 3. Use original_clrid for round-robin distribution across requests
            let start_idx = (original_clrid as usize) % hop_ids.len();

            // 4. Try each hop starting from start_idx
            for i in 0..hop_ids.len() {
                let idx = (start_idx + i) % hop_ids.len();
                let hop_id = &hop_ids[idx];

                // 5. Get the Hop
                let hop = match self.hops.get(hop_id) {
                    Some(h) => h,
                    None => continue,
                };

                // 6. Get a connection
                let conn = match hop.next_connection() {
                    Some(c) => c,
                    None => {
                        // No live connection - trigger background resolution
                        debug!(
                            segment = %segment,
                            hop = %hop_id,
                            "no live connection, attempting resolution in background"
                        );
                        let hop_clone = hop.clone();
                        tokio::spawn(async move {
                            let _ = hop_clone.resolve_address().await;
                        });
                        continue;
                    }
                };

                if conn.is_closed() {
                    continue;
                }

                // 7. Generate new correlation ID
                let new_clrid = conn.next_corr_id();

                // 8. Modify the frame in-place
                if frame.len() < clrid_offset + 4 {
                    return Err(ForwardError::Internal("frame too short for CLRID".into()));
                }

                let old_clrid =
                    u32::from_le_bytes(frame[clrid_offset..clrid_offset + 4].try_into().unwrap());

                frame[clrid_offset..clrid_offset + 4].copy_from_slice(&new_clrid.to_le_bytes());

                debug!(
                    segment = %segment,
                    hop = %hop_id,
                    old_clrid = old_clrid,
                    new_clrid = new_clrid,
                    is_write = is_write,
                    "sending request"
                );

                // 9. Try to send the frame
                let send_result = conn.send_and_wait(frame.clone(), new_clrid).await;

                match send_result {
                    Ok(response) => {
                        // Success! Restore original CLRID in-place
                        if response.len() >= clrid_offset + 4 {
                            let mut bytes = response.to_vec();
                            bytes[clrid_offset..clrid_offset + 4]
                                .copy_from_slice(&original_clrid.to_le_bytes());
                            return Ok(Bytes::from(bytes));
                        } else {
                            return Ok(response);
                        }
                    }
                    Err(e) => {
                        match &e {
                            RZError::ConnectionClosed => {
                                // Request was never sent (connection died before send)
                                // Safe to retry
                                debug!(
                                    segment = %segment,
                                    hop = %hop_id,
                                    attempt = attempt,
                                    "connection closed before send, retry safe"
                                );
                                conn.close();
                                continue;
                            }
                            _ => {
                                // Request was sent but we got an error
                                // For writes: DO NOT RETRY (could duplicate)
                                // For reads: SAFE TO RETRY (idempotent)
                                request_sent = true;

                                if is_write {
                                    debug!(
                                        segment = %segment,
                                        hop = %hop_id,
                                        error = %e,
                                        "write request failed after send, NOT retrying"
                                    );
                                    return Err(ForwardError::Timeout);
                                } else {
                                    debug!(
                                        segment = %segment,
                                        hop = %hop_id,
                                        error = %e,
                                        "read request failed after send, retry safe"
                                    );
                                    conn.close();
                                    continue;
                                }
                            }
                        }
                    }
                }
            }

            // If we get here, no hop worked on this attempt
            if is_write && request_sent {
                // Write was sent but we got no response - don't retry
                return Err(ForwardError::Timeout);
            }

            // Check if all hops are unreachable
            if attempt == self.max_retries - 1 {
                return Err(ForwardError::NetworkError);
            }

            debug!(
                segment = %segment,
                attempt = attempt,
                "retrying after attempt failure"
            );
            tokio::time::sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
        }

        // All retries exhausted
        Err(ForwardError::NetworkError)
    }

    /// Set max retries (for testing).
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }
}
