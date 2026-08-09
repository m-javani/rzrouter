use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Mutex, oneshot};
use tracing::debug;

/// Sharded demux for a single connection.
/// Each shard has its own lock to reduce contention.
pub struct Demux {
    shards: Vec<Arc<Mutex<HashMap<u32, DemuxEntry>>>>,
    num_shards: usize,
}

struct DemuxEntry {
    tx: oneshot::Sender<Result<Bytes, DemuxError>>,
    sent_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemuxError {
    Timeout,
    ConnectionClosed,
}

impl Demux {
    pub fn new(num_shards: usize) -> Self {
        let shards = (0..num_shards)
            .map(|_| Arc::new(Mutex::new(HashMap::new())))
            .collect();
        Self { shards, num_shards }
    }

    fn shard(&self, id: u32) -> usize {
        (id as usize) % self.num_shards
    }

    /// Insert a waiter for a correlation ID.
    /// Returns true if inserted, false if ID already exists.
    pub async fn insert(
        &self,
        corr_id: u32,
        tx: oneshot::Sender<Result<Bytes, DemuxError>>,
        sent_at: Instant,
    ) -> bool {
        let shard = self.shard(corr_id);
        let mut map = self.shards[shard].lock().await;
        if map.contains_key(&corr_id) {
            return false;
        }
        map.insert(corr_id, DemuxEntry { tx, sent_at });
        true
    }

    /// Remove and return the waiter for a correlation ID.
    pub async fn remove(&self, corr_id: u32) -> Option<oneshot::Sender<Result<Bytes, DemuxError>>> {
        let shard = self.shard(corr_id);
        let mut map = self.shards[shard].lock().await;
        map.remove(&corr_id).map(|entry| entry.tx)
    }

    /// Clean up expired entries.
    pub async fn cleanup(&self, max_age: Duration) {
        let threshold = Instant::now() - max_age;
        let mut timed_out = Vec::new();

        // Collect expired entries from each shard
        for shard in &self.shards {
            let map = shard.lock().await;
            for (&id, entry) in map.iter() {
                if entry.sent_at < threshold {
                    timed_out.push((id, shard.clone()));
                }
            }
        }

        // Remove and notify expired entries
        for (id, shard) in timed_out {
            let mut map = shard.lock().await;
            if let Some(entry) = map.remove(&id) {
                debug!(corr_id = id, "demux entry timed out");
                let _ = entry.tx.send(Err(DemuxError::Timeout));
            }
        }
    }

    /// Fail all pending entries (connection closed).
    pub async fn fail_all(&self) {
        let mut all_entries = Vec::new();

        for shard in &self.shards {
            let mut map = shard.lock().await;
            for (id, entry) in map.drain() {
                all_entries.push((id, entry));
            }
        }

        for (id, entry) in all_entries {
            debug!(corr_id = id, "failing demux entry (connection closed)");
            let _ = entry.tx.send(Err(DemuxError::ConnectionClosed));
        }
    }

    /// Get the number of pending entries (for metrics).
    pub async fn pending_count(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            total += shard.lock().await.len();
        }
        total
    }
}

impl Default for Demux {
    fn default() -> Self {
        Self::new(64) // 64 shards
    }
}
