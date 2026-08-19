// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzrouter.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use std::collections::{HashMap, HashSet};

use crate::protocol::serialize_get_segments;

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

    /// Replace the complete segment list for a zone.
    ///
    /// The supplied list is authoritative. Any segment previously assigned
    /// to this zone but missing from `segments` is removed.
    ///
    /// We also remove an existing mapping before inserting each segment so
    /// that segment reassignment between zones is handled correctly.
    pub fn replace_zone_segments(&mut self, zone_id: &str, segments: Vec<String>) {
        // Remove segments that used to belong to this zone but no longer do.
        self.segment_to_zone
            .retain(|_, existing_zone| existing_zone != zone_id);

        // Install the authoritative assignments.
        for segment in segments {
            self.segment_to_zone.remove(&segment);
            self.segment_to_zone.insert(segment, zone_id.to_string());
        }
    }

    /// Remove all state associated with zones that no longer exist.
    pub fn retain_valid_zones(&mut self, valid_zones: &HashSet<String>) {
        self.zone_to_routers
            .retain(|zone, _| valid_zones.contains(zone));

        self.segment_to_zone
            .retain(|_, zone| valid_zones.contains(zone));
    }

    /// Hot path: segment → zone → routers
    pub fn lookup(&self, segment: &str) -> &[String] {
        match self.segment_to_zone.get(segment) {
            Some(zone) => self
                .zone_to_routers
                .get(zone)
                .map(|routers| routers.as_slice())
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

    /// Replace the complete segment list for a shard.
    ///
    /// The supplied list is authoritative. Any segment previously assigned
    /// to this shard but missing from `segments` is removed.
    ///
    /// We also remove an existing mapping before inserting each segment so
    /// that segment reassignment between shards is handled correctly.
    pub fn replace_shard_segments(&mut self, shard_id: &str, segments: Vec<String>) {
        // Remove segments that used to belong to this shard but no longer do.
        self.segment_to_shard
            .retain(|_, existing_shard| existing_shard != shard_id);

        // Install the authoritative assignments.
        for segment in segments {
            self.segment_to_shard.remove(&segment);
            self.segment_to_shard.insert(segment, shard_id.to_string());
        }
    }

    /// Remove all segment mappings whose shard no longer exists.
    pub fn retain_valid_shards(&mut self, valid_shards: &HashSet<String>) {
        self.segment_to_shard
            .retain(|_, shard| valid_shards.contains(shard));
    }

    /// Hot path: segment → shard → bridges
    pub fn lookup(&self, segment: &str) -> &[String] {
        match self.segment_to_shard.get(segment) {
            Some(shard) => self
                .shard_to_bridges
                .get(shard)
                .map(|bridges| bridges.as_slice())
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
    /// Hot path: get next hop IDs for a segment.
    pub fn lookup(&self, segment: &str) -> &[String] {
        match self {
            RoutingSnapshot::Edge(state) => state.lookup(segment),
            RoutingSnapshot::Zone(state) => state.lookup(segment),
        }
    }

    /// Serialize all segments with count 0 using the protocol format.
    pub fn serialize_segments(&self, clrid: u32) -> Vec<u8> {
        let segments = match self {
            RoutingSnapshot::Edge(state) => state.segment_to_zone.keys(),
            RoutingSnapshot::Zone(state) => state.segment_to_shard.keys(),
        };

        serialize_get_segments(segments, clrid)
    }

    pub fn is_edge(&self) -> bool {
        matches!(self, RoutingSnapshot::Edge(_))
    }

    pub fn is_zone(&self) -> bool {
        matches!(self, RoutingSnapshot::Zone(_))
    }

    /// Get all hop IDs in this snapshot (for reconciliation).
    pub fn get_all_hop_ids(&self) -> Vec<String> {
        match self {
            RoutingSnapshot::Edge(state) => state
                .zone_to_routers
                .values()
                .flat_map(|routers| routers.iter().cloned())
                .collect(),

            RoutingSnapshot::Zone(state) => state
                .shard_to_bridges
                .values()
                .flat_map(|bridges| bridges.iter().cloned())
                .collect(),
        }
    }
}
