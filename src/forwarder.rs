// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzrouter.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use tracing::debug;

use crate::error::{ForwardError, RZError};
use crate::hop_manager::HopManager;
use crate::protocol::serialize_codecs;
use crate::routing_state::RoutingSnapshot;

const GETSEGMENTS: &str = "GETSEGMENTS";
const GETCODECS: &str = "__codecs__";

/// The forwarder handles the hot path: route → hop → connection → send → response.
pub struct Forwarder {
    routing: Arc<ArcSwap<RoutingSnapshot>>,
    hops: Arc<HopManager>,
    max_retries: usize,
    codecs: Vec<String>,
}

impl Forwarder {
    pub fn new(
        routing: Arc<ArcSwap<RoutingSnapshot>>,
        hops: Arc<HopManager>,
        codecs: Vec<String>,
    ) -> Self {
        Self {
            routing,
            hops,
            max_retries: 3,
            codecs,
        }
    }

    /// Forward a frame to the appropriate hop.
    pub async fn forward(
        &self,
        mut frame: BytesMut,
        segment: String,
        clrid_offset: usize,
        original_clrid: u32,
        is_write: bool,
    ) -> Result<Bytes, ForwardError> {
        if segment == GETSEGMENTS {
            let snapshot = self.routing.load();
            let response = snapshot.serialize_segments(original_clrid);
            return Ok(Bytes::from(response));
        }
        if segment == GETCODECS {
            let rate_features_str = self.codecs.join(",");
            let bytes = rate_features_str.into_bytes();
            // serialize response
            let serialized = serialize_codecs(original_clrid, &bytes);
            return Ok(Bytes::from(serialized));
        }

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

                let hop = match self.hops.get(hop_id) {
                    Some(h) => h,
                    None => continue,
                };

                let conn = match hop.next_connection() {
                    Some(c) => c,
                    None => {
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

                let new_clrid = conn.next_corr_id();

                // === BOUNDS CHECK / SAFE CLRID UPDATE (new) ===
                if clrid_offset + 4 > frame.len() {
                    debug!(
                        segment = %segment,
                        hop = %hop_id,
                        offset = clrid_offset,
                        frame_len = frame.len(),
                        "invalid CLRID offset"
                    );
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
                        tracing::info!(
                            "Forwarder received {} bytes, first byte: 0x{:02X}",
                            response.len(),
                            response[0]
                        );
                        // Success! Restore original CLRID in-place
                        if response.len() >= 9 {
                            let mut bytes = BytesMut::from(response.as_ref());
                            bytes[1..5].copy_from_slice(&original_clrid.to_le_bytes());
                            return Ok(bytes.freeze());
                        }
                        return Ok(response);
                    }
                    Err(e) => match &e {
                        RZError::ConnectionClosed => {
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
                    },
                }
            }

            // If we get here, no hop worked on this attempt
            if is_write && request_sent {
                return Err(ForwardError::Timeout);
            }

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
        Err(ForwardError::NetworkError)
    }

    /// Set max retries (for testing).
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }
}
