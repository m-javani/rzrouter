use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tracing::info;

use crate::config::Config;
use crate::hop::Hop;
use crate::resolver::RzPointResolver;

/// Lock-free hop registry.
pub struct HopManager {
    /// hop_id → Arc<Hop>
    hops: ArcSwap<HashMap<String, Arc<Hop>>>,
    config: Arc<Config>,
    resolver: Arc<RzPointResolver>,
}

impl HopManager {
    pub fn new(config: Arc<Config>, resolver: Arc<RzPointResolver>) -> Self {
        Self {
            hops: ArcSwap::new(Arc::new(HashMap::new())),
            config,
            resolver,
        }
    }

    /// Get or create a hop. Existing connections survive.
    pub fn get_or_create(&self, hop_id: &str) -> Arc<Hop> {
        // Fast path: read
        let map = self.hops.load();
        if let Some(hop) = map.get(hop_id) {
            return hop.clone();
        }
        drop(map);

        // Slow path: create
        let mut new_map = (**self.hops.load()).clone();
        if let std::collections::hash_map::Entry::Vacant(e) = new_map.entry(hop_id.to_string()) {
            let hop = Hop::new(hop_id, self.config.clone(), self.resolver.clone());
            info!(hop_id = %hop_id, "created new hop");
            e.insert(hop.clone());
            self.hops.store(Arc::new(new_map));
            return hop;
        }

        // Race: someone else created it
        self.hops.load().get(hop_id).unwrap().clone()
    }

    /// Lookup hop (hot path).
    pub fn get(&self, hop_id: &str) -> Option<Arc<Hop>> {
        self.hops.load().get(hop_id).cloned()
    }

    /// Retire hops that are no longer in the routing table.
    pub fn retire_unused(&self, active_hop_ids: &[String]) {
        let current_map = self.hops.load();
        let active_set: std::collections::HashSet<_> = active_hop_ids.iter().collect();

        // Find hops that need to be retired
        let to_retire: Vec<String> = current_map
            .keys()
            .filter(|id| !active_set.contains(*id))
            .cloned()
            .collect();

        if to_retire.is_empty() {
            return;
        }

        // Build new map without retired hops
        let mut new_map = (**current_map).clone();
        for id in &to_retire {
            if let Some(hop) = new_map.remove(id) {
                // Spawn async shutdown without blocking
                let hop_clone = hop.clone();
                tokio::spawn(async move {
                    hop_clone.shutdown().await;
                });
                info!(hop_id = %id, "retired hop");
            }
        }

        self.hops.store(Arc::new(new_map));
    }

    /// Get all active hop IDs (for reconciliation).
    pub fn get_all_ids(&self) -> Vec<String> {
        self.hops.load().keys().cloned().collect()
    }

    /// Get the number of hops (for metrics).
    pub fn len(&self) -> usize {
        self.hops.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.hops.load().is_empty()
    }
}
