use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::demux::{Demux, DemuxError};
use crate::error::RZError;

/// A persistent TCP connection to a hop.
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

struct ConnectionInner {
    /// Connection ID (for logging)
    id: u64,

    /// Destination address
    addr: String,

    /// Sender to the writer task
    send_tx: mpsc::Sender<Bytes>,

    /// Demux for pending requests
    demux: Arc<Demux>,

    /// Next correlation ID
    corr_id: AtomicU32,

    /// Whether the connection is closed
    closed: AtomicBool,

    /// Request timeout
    timeout: Duration,
}

impl Connection {
    /// Connect to a hop.
    pub async fn connect(
        addr: String,
        port: u16,
        config: &Config,
        conn_id: u64,
    ) -> Result<Self, RZError> {
        let full_addr = format!("{}:{}", addr, port);
        let stream = TcpStream::connect(&full_addr).await?;
        stream.set_nodelay(true)?;

        let (reader, writer) = stream.into_split();

        let demux = Arc::new(Demux::default());
        let (send_tx, send_rx) = mpsc::channel(config.max_connections.max(1024));

        let inner = Arc::new(ConnectionInner {
            id: conn_id,
            addr: full_addr.clone(),
            send_tx: send_tx.clone(),
            demux: demux.clone(),
            corr_id: AtomicU32::new(1),
            closed: AtomicBool::new(false),
            timeout: Duration::from_secs(config.request_timeout_secs),
        });

        let conn = Self {
            inner: inner.clone(),
        };

        // Start writer task
        let writer_inner = inner.clone();
        tokio::spawn(async move {
            Self::writer_task(writer_inner, writer, send_rx).await;
        });

        // Start reader task
        let reader_inner = inner.clone();
        tokio::spawn(async move {
            Self::reader_task(reader_inner, reader).await;
        });

        info!(
            conn_id = conn_id,
            addr = %full_addr,
            "connection established"
        );

        // ---------- Keepalive timer (we are the opener → we must send) ----------
        let ka_inner = inner.clone();
        let keepalive_interval = config.app_keepalive_interval(); // Duration
        if !keepalive_interval.is_zero() {
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(keepalive_interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                ticker.tick().await; // skip the immediate first tick

                let ka_frame = Bytes::from(crate::protocol::build_keepalive_frame(0));

                loop {
                    ticker.tick().await;

                    if ka_inner.closed.load(Ordering::Acquire) {
                        break;
                    }

                    // Fire-and-forget through the existing write channel
                    if ka_inner.send_tx.send(ka_frame.clone()).await.is_err() {
                        break; // channel closed → connection dying
                    }
                }
            });
        }

        Ok(conn)
    }

    /// Writer task: sends frames to the TCP socket.
    async fn writer_task(
        inner: Arc<ConnectionInner>,
        mut writer: OwnedWriteHalf,
        mut rx: mpsc::Receiver<Bytes>,
    ) {
        while let Some(frame) = rx.recv().await {
            if inner.closed.load(Ordering::Acquire) {
                break;
            }

            if let Err(e) = writer.write_all(&frame).await {
                debug!(
                    conn_id = inner.id,
                    addr = %inner.addr,
                    error = %e,
                    "write failed"
                );
                inner.closed.store(true, Ordering::Release);
                break;
            }

            // flush after each write to ensure data is sent
            if let Err(e) = writer.flush().await {
                debug!(
                    conn_id = inner.id,
                    addr = %inner.addr,
                    error = %e,
                    "flush failed"
                );
                inner.closed.store(true, Ordering::Release);
                break;
            }
        }

        // Connection is closed, fail all pending requests
        inner.demux.fail_all().await;
        inner.closed.store(true, Ordering::Release);

        debug!(
            conn_id = inner.id,
            addr = %inner.addr,
            "writer task stopped"
        );
    }

    /// Reader task: reads responses from the TCP socket.
    async fn reader_task(inner: Arc<ConnectionInner>, mut reader: OwnedReadHalf) {
        let mut buf = BytesMut::with_capacity(8192);

        loop {
            // Need at least 9 bytes for response header: 0xFF + clrid(4) + len(4)
            while buf.len() < 9 {
                match reader.read_buf(&mut buf).await {
                    Ok(0) => {
                        // EOF
                        debug!(
                            conn_id = inner.id,
                            addr = %inner.addr,
                            "connection closed by peer"
                        );
                        inner.closed.store(true, Ordering::Release);
                        inner.demux.fail_all().await;
                        return;
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        debug!(
                            conn_id = inner.id,
                            addr = %inner.addr,
                            error = %e,
                            "read failed"
                        );
                        inner.closed.store(true, Ordering::Release);
                        inner.demux.fail_all().await;
                        return;
                    }
                }
            }

            // Validate response magic
            if buf[0] != 0xFF {
                warn!(
                    conn_id = inner.id,
                    addr = %inner.addr,
                    "invalid response magic: 0x{:02X}",
                    buf[0]
                );
                // Try to recover by advancing one byte
                buf.advance(1);
                continue;
            }

            let clr_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let payload_len = u32::from_le_bytes(buf[5..9].try_into().unwrap()) as usize;
            let frame_len = 9 + payload_len;

            // Wait for full frame
            while buf.len() < frame_len {
                match reader.read_buf(&mut buf).await {
                    Ok(0) => {
                        debug!(
                            conn_id = inner.id,
                            addr = %inner.addr,
                            "connection closed by peer (partial frame)"
                        );
                        inner.closed.store(true, Ordering::Release);
                        inner.demux.fail_all().await;
                        return;
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        debug!(
                            conn_id = inner.id,
                            addr = %inner.addr,
                            error = %e,
                            "read failed (partial frame)"
                        );
                        inner.closed.store(true, Ordering::Release);
                        inner.demux.fail_all().await;
                        return;
                    }
                }
            }

            // Extract the complete frame
            let frame = buf.split_to(frame_len).freeze();

            // Find the waiter
            let waiter = inner.demux.remove(clr_id).await;

            if let Some(tx) = waiter {
                debug!(conn_id = inner.id, corr_id = clr_id, "response received");
                let _ = tx.send(Ok(frame));
            } else {
                debug!(
                    conn_id = inner.id,
                    corr_id = clr_id,
                    "unknown correlation id"
                );
                // Drop the frame - no waiter
            }
        }
    }

    /// Send a frame and wait for the response.
    /// The frame must already have the new CLRID set at the correct offset.
    pub async fn send_and_wait(&self, frame: BytesMut, new_clrid: u32) -> Result<Bytes, RZError> {
        if self.is_closed() {
            return Err(RZError::ConnectionClosed);
        }

        // Register the waiter BEFORE sending to avoid race conditions
        let (tx, rx) = oneshot::channel();
        if !self.inner.demux.insert(new_clrid, tx, Instant::now()).await {
            return Err(RZError::Validation(format!(
                "correlation id {} already in use",
                new_clrid
            )));
        }

        // Send the frame
        let bytes = frame.freeze();
        if let Err(_) = self.inner.send_tx.try_send(bytes.clone()) {
            // Send failed - connection is dead, clean up demux
            self.inner.demux.remove(new_clrid).await;
            return Err(RZError::ConnectionClosed);
        }

        // Wait for response with timeout
        match timeout(self.inner.timeout, rx).await {
            Ok(Ok(Ok(response))) => Ok(response),
            Ok(Ok(Err(e))) => {
                // Demux error (timeout or connection closed)
                match e {
                    DemuxError::Timeout => Err(RZError::Timeout),
                    DemuxError::ConnectionClosed => Err(RZError::ConnectionClosed),
                }
            }
            Ok(Err(_)) => {
                // Sender dropped (shouldn't happen if demux cleanup works)
                Err(RZError::ConnectionClosed)
            }
            Err(_) => {
                // Timeout - clean up the demux entry
                self.inner.demux.remove(new_clrid).await;
                Err(RZError::Timeout)
            }
        }
    }

    /// Send a frame without waiting for a response (fire-and-forget).
    pub async fn send_raw(&self, frame: Bytes) -> Result<(), RZError> {
        if self.is_closed() {
            return Err(RZError::ConnectionClosed);
        }

        self.inner
            .send_tx
            .send(frame)
            .await
            .map_err(|_| RZError::ConnectionClosed)
    }

    /// Check if the connection is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Close the connection.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }

    /// Generate the next correlation ID.
    pub fn next_corr_id(&self) -> u32 {
        // Wraparound: avoid 0 (reserved) and avoid currently active IDs
        loop {
            let id = self.inner.corr_id.fetch_add(1, Ordering::Relaxed);
            if id != 0 {
                return id;
            }
        }
    }

    /// Get the demux for external inspection (metrics, etc.)
    pub fn demux(&self) -> &Arc<Demux> {
        &self.inner.demux
    }
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Mark as closed if not already
        self.inner.closed.store(true, Ordering::Release);
    }
}
