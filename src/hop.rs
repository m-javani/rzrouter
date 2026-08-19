// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzrouter.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arc_swap::{ArcSwap, ArcSwapOption};
use tracing::{debug, warn};

use crate::config::Config;
use crate::connection::Connection;
use crate::resolver::RzPointResolver;

/// A connection set that can be atomically replaced.
#[derive(Clone, Default)]
pub struct ConnectionSet {
    pub connections: Vec<Arc<Connection>>,
}

impl ConnectionSet {
    /// Pick the next healthy connection using round-robin.
    pub fn pick(&self, cursor: usize) -> Option<Arc<Connection>> {
        let n = self.connections.len();
        if n == 0 {
            return None;
        }
        for i in 0..n {
            let idx = (cursor + i) % n;
            let conn = &self.connections[idx];
            if !conn.is_closed() {
                return Some(conn.clone());
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.connections.len()
    }

    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    /// Get live connections count
    pub fn live_count(&self) -> usize {
        self.connections.iter().filter(|c| !c.is_closed()).count()
    }
}

/// Hop state
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum HopState {
    Healthy,
    Recovering,
    Unavailable,
}

/// Atomic hop state
pub struct AtomicHopState {
    state: AtomicUsize,
}

impl AtomicHopState {
    pub fn new(state: HopState) -> Self {
        let val = match state {
            HopState::Healthy => 0,
            HopState::Recovering => 1,
            HopState::Unavailable => 2,
        };
        Self {
            state: AtomicUsize::new(val),
        }
    }

    pub fn load(&self) -> HopState {
        match self.state.load(Ordering::Acquire) {
            0 => HopState::Healthy,
            1 => HopState::Recovering,
            _ => HopState::Unavailable,
        }
    }

    pub fn store(&self, state: HopState) {
        let val = match state {
            HopState::Healthy => 0,
            HopState::Recovering => 1,
            HopState::Unavailable => 2,
        };
        self.state.store(val, Ordering::Release);
    }

    pub fn compare_exchange(&self, current: HopState, new: HopState) -> Result<(), HopState> {
        let current_val = match current {
            HopState::Healthy => 0,
            HopState::Recovering => 1,
            HopState::Unavailable => 2,
        };
        let new_val = match new {
            HopState::Healthy => 0,
            HopState::Recovering => 1,
            HopState::Unavailable => 2,
        };

        match self
            .state
            .compare_exchange(current_val, new_val, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(actual) => {
                let actual_state = match actual {
                    0 => HopState::Healthy,
                    1 => HopState::Recovering,
                    _ => HopState::Unavailable,
                };
                Err(actual_state)
            }
        }
    }
}

/// A Hop represents one logical destination (zone router or bridge).
pub struct Hop {
    pub id: String,

    /// Cached address from RzPoint (stored as Arc<String> for ArcSwapOption)
    pub address: ArcSwapOption<String>,

    /// Connection set (atomically replaceable)
    pub connections: ArcSwap<ConnectionSet>,

    /// Round-robin cursor
    pub rr: AtomicUsize,

    /// Health state
    pub state: AtomicHopState,

    /// Config
    config: Arc<Config>,

    /// RzPoint resolver
    resolver: Arc<RzPointResolver>,

    /// Background task cancellation token
    cancel: tokio_util::sync::CancellationToken,
}

impl Hop {
    pub fn new(id: &str, config: Arc<Config>, resolver: Arc<RzPointResolver>) -> Arc<Self> {
        let cancel = tokio_util::sync::CancellationToken::new();
        let hop = Arc::new(Self {
            id: id.to_string(),
            address: ArcSwapOption::new(None),
            connections: ArcSwap::new(Arc::new(ConnectionSet {
                connections: Vec::new(),
            })),
            rr: AtomicUsize::new(0),
            state: AtomicHopState::new(HopState::Healthy),
            config,
            resolver,
            cancel: cancel.clone(),
        });

        // Start background maintenance
        let maintenance_hop = hop.clone();
        tokio::spawn(async move {
            maintenance_hop.maintenance_loop().await;
        });

        hop
    }

    /// Hot path: select the next healthy connection.
    pub fn next_connection(&self) -> Option<Arc<Connection>> {
        let set = self.connections.load();
        let cursor = self.rr.fetch_add(1, Ordering::Relaxed);
        set.pick(cursor)
    }

    /// Get the current address, resolving if needed.
    pub async fn resolve_address(&self) -> Option<String> {
        // Fast path: already resolved
        if let Some(addr) = self.address.load().as_ref() {
            return Some(addr.to_string());
        }

        // Slow path: resolve
        self.ensure_address().await
    }

    /// Background maintenance loop.
    async fn maintenance_loop(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = interval.tick() => {
                    self.maintenance_tick().await;
                }
            }
        }
    }

    async fn maintenance_tick(&self) {
        let state = self.state.load();

        match state {
            HopState::Healthy => {
                // Ensure we have enough connections
                self.ensure_connections().await;
            }
            HopState::Recovering => {
                // Try to recover
                if let Some(addr) = self.address.load().as_ref() {
                    if self.try_reconnect(addr).await {
                        self.state.store(HopState::Healthy);
                    } else {
                        // Try RzPoint
                        if let Ok(new_addr) = self.resolver.resolve(&self.id).await {
                            debug!(hop_id = %self.id, addr = %new_addr, "rzpoint resolved new address");
                            self.address.store(Some(Arc::new(new_addr.clone())));
                            if self.try_reconnect(&new_addr).await {
                                self.state.store(HopState::Healthy);
                            }
                        }
                    }
                } else {
                    // No address at all, resolve from RzPoint
                    if let Ok(addr) = self.resolver.resolve(&self.id).await {
                        debug!(hop_id = %self.id, addr = %addr, "rzpoint resolved initial address");
                        self.address.store(Some(Arc::new(addr.clone())));
                        if self.try_reconnect(&addr).await {
                            self.state.store(HopState::Healthy);
                        }
                    }
                }
            }
            HopState::Unavailable => {
                // Wait for a backoff period before trying again
                // The recovery task will try again later
                if self.address.load().is_some() {
                    self.state.store(HopState::Recovering);
                }
            }
        }
    }

    async fn ensure_connections(&self) {
        let target = self.config.conn_per_hop.max(1);
        let set = self.connections.load();
        let live = set.live_count();

        if live < target {
            // Get address, resolving if needed
            let addr = match self.address.load().as_ref() {
                Some(a) => a.to_string(),
                None => {
                    // No address, try to resolve
                    match self.resolver.resolve(&self.id).await {
                        Ok(addr) => {
                            self.address.store(Some(Arc::new(addr.clone())));
                            addr
                        }
                        Err(e) => {
                            warn!(hop_id = %self.id, error = %e, "failed to resolve address");
                            return;
                        }
                    }
                }
            };

            let to_create = target - live;
            let mut new_conns: Vec<Arc<Connection>> = set
                .connections
                .iter()
                .filter(|c| !c.is_closed())
                .cloned()
                .collect();
            let mut conn_counter = 0u64;

            for _ in 0..to_create {
                let conn_id = conn_counter;
                conn_counter += 1;

                match Connection::connect(
                    addr.clone(),
                    self.config.hop_tcp_port,
                    &self.config,
                    conn_id,
                )
                .await
                {
                    Ok(conn) => {
                        new_conns.push(Arc::new(conn));
                        debug!(
                            hop_id = %self.id,
                            "created new connection to {}",
                            addr
                        );
                    }
                    Err(e) => {
                        warn!(
                            hop_id = %self.id,
                            addr = %addr,
                            error = %e,
                            "failed to create connection"
                        );
                        if new_conns.is_empty() && to_create > 1 {
                            // If we failed to create any, mark as recovering
                            self.state.store(HopState::Recovering);
                        }
                    }
                }
            }

            if !new_conns.is_empty() {
                let new_set = ConnectionSet {
                    connections: new_conns,
                };
                self.connections.store(Arc::new(new_set));
            }
        }
    }

    async fn try_reconnect(&self, addr: &str) -> bool {
        // Try to create one connection to test connectivity
        let conn_id = 999_999; // Use a high number for reconnect attempts

        match Connection::connect(
            addr.to_string(),
            self.config.hop_tcp_port,
            &self.config,
            conn_id,
        )
        .await
        {
            Ok(conn) => {
                // Replace the connection set with this one connection
                let new_set = ConnectionSet {
                    connections: vec![Arc::new(conn)],
                };
                self.connections.store(Arc::new(new_set));
                debug!(hop_id = %self.id, addr = %addr, "reconnected successfully");
                true
            }
            Err(e) => {
                warn!(hop_id = %self.id, addr = %addr, error = %e, "reconnect failed");
                false
            }
        }
    }

    /// Public method to trigger recovery (called when connection dies).
    pub async fn ensure_address(&self) -> Option<String> {
        // Try to get address, resolve if needed
        if let Some(addr) = self.address.load().as_ref() {
            return Some(addr.to_string());
        }

        // No address, try RzPoint
        match self.resolver.resolve(&self.id).await {
            Ok(addr) => {
                self.address.store(Some(Arc::new(addr.clone())));
                Some(addr)
            }
            Err(e) => {
                warn!(hop_id = %self.id, error = %e, "failed to resolve address");
                self.state.store(HopState::Unavailable);
                None
            }
        }
    }

    /// Shutdown the hop and its connections.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        // Close all connections
        let set = self.connections.load();
        for conn in &set.connections {
            conn.close();
        }
    }
}

impl Drop for Hop {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
