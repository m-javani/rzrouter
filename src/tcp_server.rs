use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::error::RZError;
use crate::forwarder::Forwarder;
use crate::protocol::{build_error_response, try_decode_router};

pub struct TcpServer {
    config: Arc<Config>,
    cancel: CancellationToken,
    active: Arc<AtomicUsize>,
    forwarder: Arc<Forwarder>,
}

impl TcpServer {
    pub fn new(config: Arc<Config>, cancel: CancellationToken, forwarder: Arc<Forwarder>) -> Self {
        Self {
            config,
            cancel,
            active: Arc::new(AtomicUsize::new(0)),
            forwarder,
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
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    m = close_rx.recv() => {
                        if m.is_none() { break; }
                        active.fetch_sub(1, Ordering::Relaxed);
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

                    let forwarder = self.forwarder.clone();
                    let cancel = self.cancel.clone();
                    let close_tx = close_tx.clone();

                    tokio::spawn(async move {
                        let result = handle_connection(stream, forwarder, cancel).await;
                        if let Err(e) = result {
                            debug!(addr = %addr, "connection error: {}", e);
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
    forwarder: Arc<Forwarder>,
    cancel: CancellationToken,
) -> Result<(), RZError> {
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();

    let mut buf = BytesMut::with_capacity(16 * 1024);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            read = reader.read_buf(&mut buf) => {
                match read {
                    Ok(0) => break, // EOF
                    Ok(_) => {}
                    Err(e) => {
                        debug!("read error: {}", e);
                        break;
                    }
                }

                loop {
                    // Parse the frame - this borrows from buf immutably
                    let parse_result = try_decode_router(&buf)?;

                    let (frame_len, segment, clrid_offset, original_clrid, is_write) = match parse_result {
                        None => break, // Need more bytes
                        Some(metadata) => {
                            // Extract ALL data from metadata BEFORE mutating buf
                            (
                                metadata.frame_len,
                                metadata.segment.to_string(),
                                metadata.clrid_offset,
                                metadata.original_clrid,
                                metadata.is_write
                            )
                        }
                    };

                    // Now it's safe to mutate buf because metadata is dropped
                    let frame = buf.split_to(frame_len);

                    // Forward the frame
                    match forwarder
                        .forward(frame, segment, clrid_offset, original_clrid, is_write)
                        .await
                    {
                        Ok(response) => {
                            if let Err(e) = writer.write_all(&response).await {
                                debug!("write error: {}", e);
                                break;
                            }
                            if let Err(e) = writer.flush().await {
                                debug!("flush error: {}", e);
                                break;
                            }
                        }
                        Err(forward_error) => {
                            use crate::error::ForwardError;

                            match forward_error {
                                ForwardError::UnknownSegment(msg) => {
                                    // Send error response: unknown segment
                                    debug!(msg = %msg, "unknown segment");
                                    let error_frame = build_error_response(original_clrid, 1); // 1 = unknown segment
                                    if let Err(e) = writer.write_all(&error_frame).await {
                                        debug!("write error: {}", e);
                                        break;
                                    }
                                    if let Err(e) = writer.flush().await {
                                        debug!("flush error: {}", e);
                                        break;
                                    }
                                }
                                ForwardError::NetworkError => {
                                    // Send error response: network error
                                    debug!("network error");
                                    let error_frame = build_error_response(original_clrid, 2); // 2 = network error
                                    if let Err(e) = writer.write_all(&error_frame).await {
                                        debug!("write error: {}", e);
                                        break;
                                    }
                                    if let Err(e) = writer.flush().await {
                                        debug!("flush error: {}", e);
                                        break;
                                    }
                                }
                                ForwardError::Timeout => {
                                    // Timeout - drop silently, client will timeout
                                    debug!("request timed out, dropping");
                                    // Connection stays open
                                }
                                ForwardError::Internal(msg) => {
                                    // Internal error - drop silently, client will timeout
                                    debug!("internal error: {}", msg);
                                    // Connection stays open
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
