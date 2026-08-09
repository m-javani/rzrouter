use std::collections::{HashMap, HashSet};

/// Edge Router State
///
/// segment → zone → [router IDs]
#[derive(Clone, Default)]
pub struct EdgeState {
    pub segment_to_zone: HashMap<String, String>,
    pub zone_to_routers: HashMap<String, Vec<String>>,
}

impl EdgeState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_zone_routers(&mut self, zone_id: &str, routers: Vec<String>) {
        self.zone_to_routers.insert(zone_id.to_string(), routers);
    }

    pub fn set_segment_zone(&mut self, segment: String, zone_id: String) {
        self.segment_to_zone.insert(segment, zone_id);
    }

    pub fn retain_valid_zones(&mut self, valid_zones: &HashSet<String>) {
        self.zone_to_routers.retain(|z, _| valid_zones.contains(z));
        self.segment_to_zone.retain(|_, z| valid_zones.contains(z));
    }

    /// Hot path: segment → zone → routers
    pub fn lookup(&self, segment: &str) -> &[String] {
        match self.segment_to_zone.get(segment) {
            Some(zone) => self
                .zone_to_routers
                .get(zone)
                .map(|v| v.as_slice())
                .unwrap_or(&[]),
            None => &[],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.segment_to_zone.is_empty() && self.zone_to_routers.is_empty()
    }
}

/// Zone Router State
///
/// segment → shard → [bridge IDs]
#[derive(Clone, Default)]
pub struct ZoneState {
    pub segment_to_shard: HashMap<String, String>,
    pub shard_to_bridges: HashMap<String, Vec<String>>,
}

impl ZoneState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_shard_bridges(&mut self, shard_id: &str, bridges: Vec<String>) {
        self.shard_to_bridges.insert(shard_id.to_string(), bridges);
    }

    pub fn set_segment_shard(&mut self, segment: String, shard_id: String) {
        self.segment_to_shard.insert(segment, shard_id);
    }

    /// Hot path: segment → shard → bridges
    pub fn lookup(&self, segment: &str) -> &[String] {
        match self.segment_to_shard.get(segment) {
            Some(shard) => self
                .shard_to_bridges
                .get(shard)
                .map(|v| v.as_slice())
                .unwrap_or(&[]),
            None => &[],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.segment_to_shard.is_empty() && self.shard_to_bridges.is_empty()
    }
}

/// Routing Snapshot (payload for ArcSwap)
#[derive(Clone)]
pub enum RoutingSnapshot {
    Edge(EdgeState),
    Zone(ZoneState),
}

impl RoutingSnapshot {
    /// Hot path: get next hop IDs for a segment
    pub fn lookup(&self, segment: &str) -> &[String] {
        match self {
            RoutingSnapshot::Edge(state) => state.lookup(segment),
            RoutingSnapshot::Zone(state) => state.lookup(segment),
        }
    }

    pub fn is_edge(&self) -> bool {
        matches!(self, RoutingSnapshot::Edge(_))
    }

    pub fn is_zone(&self) -> bool {
        matches!(self, RoutingSnapshot::Zone(_))
    }

    /// Get all hop IDs in this snapshot (for reconciliation)
    pub fn get_all_hop_ids(&self) -> Vec<String> {
        match self {
            RoutingSnapshot::Edge(state) => {
                let mut ids = Vec::new();
                for routers in state.zone_to_routers.values() {
                    ids.extend(routers.iter().cloned());
                }
                ids
            }
            RoutingSnapshot::Zone(state) => {
                let mut ids = Vec::new();
                for bridges in state.shard_to_bridges.values() {
                    ids.extend(bridges.iter().cloned());
                }
                ids
            }
        }
    }
}
