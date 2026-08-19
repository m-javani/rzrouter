// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzrouter.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::error::{ForwardError, RZError};
use crate::forwarder::Forwarder;
use crate::metrics::SharedMetrics;
use crate::protocol::{
    KEEPALIVE_SEGMENT, build_error_response, find_next_router_magic, try_decode_router,
};

pub struct TcpServer {
    config: Arc<Config>,
    cancel: CancellationToken,
    active: Arc<AtomicUsize>,
    forwarder: Arc<Forwarder>,
    metrics: SharedMetrics,
}

impl TcpServer {
    pub fn new(
        config: Arc<Config>,
        cancel: CancellationToken,
        forwarder: Arc<Forwarder>,
        metrics: SharedMetrics,
    ) -> Self {
        Self {
            config,
            cancel,
            active: Arc::new(AtomicUsize::new(0)),
            forwarder,
            metrics,
        }
    }

    pub async fn run(&self, listener: TcpListener) -> Result<(), std::io::Error> {
        info!(
            "router tcp server listening on port {}",
            self.config.tcp_port
        );

        let (close_tx, mut close_rx) = mpsc::unbounded_channel::<()>();
        let cancel = self.cancel.clone();
        let active = self.active.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    m = close_rx.recv() => {
                        if m.is_none() { break; }
                        active.fetch_sub(1, Ordering::Relaxed);
                        metrics.router.connections_closed.increment(1);
                    }
                }
            }
        });

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    info!("tcp server shutting down");
                    return Ok(());
                }
                acc = listener.accept() => {
                    let (stream, addr) = match acc {
                        Ok((s, a)) => (s, a),
                        Err(e) => {
                            error!("accept failed: {}", e);
                            continue;
                        }
                    };

                    if self.active.load(Ordering::Relaxed) >= self.config.max_connections {
                        warn!("max connections reached, dropping connection from {}", addr);
                        drop(stream);
                        continue;
                    }

                    self.active.fetch_add(1, Ordering::Relaxed);
                    self.metrics.router.connections_opened.increment(1);

                    let forwarder = self.forwarder.clone();
                    let cancel = self.cancel.clone();
                    let close_tx = close_tx.clone();
                    let config = self.config.clone();
                    let metrics = self.metrics.clone();

                    tokio::spawn(async move {
                        let start = Instant::now();
                        let result = handle_connection(
                            stream,
                            addr,
                            forwarder,
                            cancel,
                            config,
                            metrics.clone(),
                        )
                        .await;

                        let duration = start.elapsed();
                        if let Err(e) = result {
                            debug!(%addr, duration = ?duration, "connection ended with error: {}", e);
                        } else {
                            debug!(%addr, duration = ?duration, "connection closed cleanly");
                        }

                        let _ = close_tx.send(());
                    });
                }
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    addr: std::net::SocketAddr,
    forwarder: Arc<Forwarder>,
    cancel: CancellationToken,
    config: Arc<Config>,
    metrics: SharedMetrics,
) -> Result<(), RZError> {
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();

    let mut buf = BytesMut::with_capacity(16 * 1024);
    let mut last_frame_time = Instant::now();

    let frame_timeout = config.frame_timeout();
    let idle_timeout = config.idle_timeout();
    let max_buffer = config.max_buffer_size;
    let max_frame = config.max_frame_size;

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                debug!(%addr, "cancellation requested");
                break;
            }

            // Read with overall idle timeout
            read = timeout(idle_timeout, reader.read_buf(&mut buf)) => {
                match read {
                    Ok(Ok(0)) => {
                        debug!(%addr, "client closed connection");
                        break;
                    }
                    Ok(Ok(n)) => {
                        metrics.router.bytes_received.increment(n as u64);
                    }
                    Ok(Err(e)) => {
                        debug!(%addr, "read error: {}", e);
                        break;
                    }
                    Err(_) => {
                        // idle timeout – remote side stopped sending (including keepalives)
                        warn!(%addr, "idle timeout, closing");
                        metrics.router.timeouts.increment(1);
                        break;
                    }
                }

                // Process as many complete frames as possible
                loop {
                    if buf.len() > max_buffer {
                        warn!(
                            %addr,
                            size = buf.len(),
                            max = max_buffer,
                            "buffer exceeded max size, dropping connection"
                        );
                        metrics.router.client_errors.increment(1);
                        return Ok(());
                    }

                    match try_decode_router(&buf) {
                        Ok(None) => {
                            // Incomplete frame – enforce per-frame timeout
                            if last_frame_time.elapsed() > frame_timeout {
                                warn!(%addr, "frame completion timeout (slow client)");
                                metrics.router.timeouts.increment(1);
                                return Ok(());
                            }
                            break; // wait for more bytes
                        }

                        Ok(Some(frame)) => {
                            if frame.frame_len > max_frame {
                                warn!(
                                    %addr,
                                    size = frame.frame_len,
                                    max = max_frame,
                                    "frame exceeds max_frame_size, dropping connection"
                                );
                                metrics.router.client_errors.increment(1);
                                return Ok(());
                            }

                            last_frame_time = Instant::now();
                            metrics.router.frames_received.increment(1);

                            let frame_len = frame.frame_len;
                            let segment = frame.segment.to_string();
                            let clrid_offset = frame.clrid_offset;
                            let original_clrid = frame.original_clrid;
                            let is_write = frame.is_write;

                            let frame_bytes = buf.split_to(frame_len);

                            // Incoming keepalive from the side that opened the connection
                            if segment == KEEPALIVE_SEGMENT {
                                metrics.router.keepalives_received.increment(1);
                                // Just treat as activity – do not reply
                                continue;
                            }

                            // Real request
                            match forwarder
                                .forward(frame_bytes, segment, clrid_offset, original_clrid, is_write)
                                .await
                            {
                                Ok(response) => {
                                    metrics.router.frames_forwarded.increment(1);
                                    metrics.router.bytes_sent.increment(response.len() as u64);

                                    if let Err(e) = writer.write_all(&response).await {
                                        debug!(%addr, "write error: {}", e);
                                        return Ok(());
                                    }
                                    if let Err(e) = writer.flush().await {
                                        debug!(%addr, "flush error: {}", e);
                                        return Ok(());
                                    }
                                }

                                Err(fe) => {
                                    let (code, metric) = match fe {
                                        ForwardError::UnknownSegment(msg) => {
                                            debug!(%addr, %msg, "unknown segment");
                                            (1u8, &metrics.router.unknown_segment)
                                        }
                                        ForwardError::NetworkError => {
                                            debug!(%addr, "network error");
                                            (2, &metrics.router.network_errors)
                                        }
                                        ForwardError::Timeout => {
                                            debug!(%addr, "forward timeout");
                                            (3, &metrics.router.timeouts)
                                        }
                                        ForwardError::Internal(msg) => {
                                            debug!(%addr, %msg, "internal error");
                                            (4, &metrics.router.internal_errors)
                                        }
                                    };

                                    metric.increment(1);

                                    let error_frame = build_error_response(original_clrid, code);
                                    metrics.router.bytes_sent.increment(error_frame.len() as u64);

                                    if let Err(e) = writer.write_all(&error_frame).await {
                                        debug!(%addr, "error response write failed: {}", e);
                                        return Ok(());
                                    }
                                    let _ = writer.flush().await;
                                }
                            }
                        }

                        Err(e) => {
                            // Bad magic / structural error → try to resync
                            metrics.router.client_errors.increment(1);
                            metrics.router.resyncs.increment(1);
                            debug!(%addr, error = %e, "parse error, attempting resync");

                            if buf.is_empty() {
                                break;
                            }

                            buf.advance(1);

                            match find_next_router_magic(&buf, 0) {
                                Some(pos) => {
                                    if pos > 0 {
                                        debug!(%addr, discarded = pos, "resync: skipping garbage");
                                        buf.advance(pos);
                                    }
                                    continue;
                                }
                                None => {
                                    // Keep a small window in case magic is split across reads
                                    if buf.len() > 64 {
                                        let keep = 8.min(buf.len());
                                        let start = buf.len() - keep;
                                        buf.advance(start);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
