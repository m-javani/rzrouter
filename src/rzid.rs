use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::config::{Config, RouterMode};
use crate::error::RZError;
use crate::hop_manager::HopManager;
use crate::routing_state::{EdgeState, RoutingSnapshot, ZoneState};

// ---------------------------------------------------------------------------
// Wire types (matches RzID API)
// ---------------------------------------------------------------------------
#[derive(Debug, serde::Serialize)]
struct RegisterRequest {
    kind: String,
    id: String,
    zone: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    shard: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VersionManifest {
    pub global_version: u64,
    pub versions: HashMap<String, u64>,
}

#[derive(Debug, Deserialize)]
pub struct ZoneRoutersResponse {
    pub version: u64,
    pub routers: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ZoneSegmentsResponse {
    pub version: u64,
    pub segments: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ZoneShardsResponse {
    pub version: u64,
    pub shards: HashMap<String, ShardBridges>,
}

#[derive(Debug, Deserialize)]
pub struct ShardBridges {
    pub bridges: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ShardSegmentsResponse {
    pub version: u64,
    pub zone: String,
    pub segments: Vec<String>,
}

// ---------------------------------------------------------------------------
// RzID Client
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct RzidClient {
    client: Client,
    base_url: String,
    mode: RouterMode,
    router_id: String,
    zone_id: String,
    local_versions: Arc<RwLock<HashMap<String, u64>>>,
}

impl RzidClient {
    pub fn new(cfg: &Config) -> Result<Self, RZError> {
        if cfg.mode == RouterMode::Zone {
            if cfg.zone_id.is_none() || cfg.router_id.is_none() {
                return Err(RZError::Config(
                    "zone mode requires zone_id and router_id".into(),
                ));
            }
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| RZError::System(format!("rzid client: {e}")))?;

        let base_url = if cfg.rzid_addr.starts_with("http") {
            cfg.rzid_addr.trim_end_matches('/').to_string()
        } else {
            format!("http://{}", cfg.rzid_addr.trim_end_matches('/'))
        };

        Ok(Self {
            client,
            base_url,
            mode: cfg.mode,
            router_id: cfg.router_id.clone().unwrap_or_default(),
            zone_id: cfg.zone_id.clone().unwrap_or_default(),
            local_versions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    // -----------------------------------------------------------------------
    // Registration / Heartbeat
    // -----------------------------------------------------------------------

    pub async fn send_heartbeat(&self) -> Result<(), RZError> {
        if self.mode != RouterMode::Zone {
            return Ok(());
        }

        let url = format!("{}/register", self.base_url);
        let body = RegisterRequest {
            kind: "router".into(),
            id: self.router_id.clone(),
            zone: self.zone_id.clone(),
            shard: None,
        };

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| RZError::Http(format!("rzid register: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(RZError::Http(format!("rzid register {status}: {text}")));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Version manifest
    // -----------------------------------------------------------------------

    pub async fn fetch_version_manifest(&self) -> Result<VersionManifest, RZError> {
        let url = format!("{}/versions", self.base_url);
        let resp: VersionManifest = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| RZError::Http(format!("versions: {e}")))?
            .error_for_status()
            .map_err(|e| RZError::Http(format!("versions: {e}")))?
            .json()
            .await
            .map_err(|e| RZError::Http(format!("versions json: {e}")))?;
        Ok(resp)
    }

    // -----------------------------------------------------------------------
    // Edge Router Queries
    // -----------------------------------------------------------------------

    pub async fn fetch_zone_routers(&self, zone_id: &str) -> Result<(u64, Vec<String>), RZError> {
        let url = format!("{}/zones/{}/routers", self.base_url, zone_id);
        let resp: ZoneRoutersResponse = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| RZError::Http(format!("zone routers: {e}")))?
            .error_for_status()
            .map_err(|e| RZError::Http(format!("zone routers: {e}")))?
            .json()
            .await
            .map_err(|e| RZError::Http(format!("zone routers json: {e}")))?;
        Ok((resp.version, resp.routers))
    }

    pub async fn fetch_zone_segments(&self, zone_id: &str) -> Result<(u64, Vec<String>), RZError> {
        let url = format!("{}/zones/{}/segments", self.base_url, zone_id);
        let resp: ZoneSegmentsResponse = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| RZError::Http(format!("zone segments: {e}")))?
            .error_for_status()
            .map_err(|e| RZError::Http(format!("zone segments: {e}")))?
            .json()
            .await
            .map_err(|e| RZError::Http(format!("zone segments json: {e}")))?;
        Ok((resp.version, resp.segments))
    }

    // -----------------------------------------------------------------------
    // Zone Router Queries
    // -----------------------------------------------------------------------

    pub async fn fetch_zone_shards(
        &self,
        zone_id: &str,
    ) -> Result<(u64, HashMap<String, Vec<String>>), RZError> {
        let url = format!("{}/zones/{}/shards", self.base_url, zone_id);
        let resp: ZoneShardsResponse = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| RZError::Http(format!("zone shards: {e}")))?
            .error_for_status()
            .map_err(|e| RZError::Http(format!("zone shards: {e}")))?
            .json()
            .await
            .map_err(|e| RZError::Http(format!("zone shards json: {e}")))?;

        let shards = resp
            .shards
            .into_iter()
            .map(|(k, v)| (k, v.bridges))
            .collect();
        Ok((resp.version, shards))
    }

    pub async fn fetch_shard_segments(
        &self,
        shard_id: &str,
    ) -> Result<(u64, String, Vec<String>), RZError> {
        let url = format!("{}/shards/{}/segments", self.base_url, shard_id);
        let resp: ShardSegmentsResponse = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| RZError::Http(format!("shard segments: {e}")))?
            .error_for_status()
            .map_err(|e| RZError::Http(format!("shard segments: {e}")))?
            .json()
            .await
            .map_err(|e| RZError::Http(format!("shard segments json: {e}")))?;
        Ok((resp.version, resp.zone, resp.segments))
    }

    // -----------------------------------------------------------------------
    // Full Sync
    // -----------------------------------------------------------------------

    pub async fn sync_routing_state(
        &self,
        current_snapshot: Option<&RoutingSnapshot>,
        hop_manager: &HopManager,
    ) -> Result<Option<RoutingSnapshot>, RZError> {
        let manifest = self.fetch_version_manifest().await?;

        let changed_keys = self.detect_changes(&manifest).await;
        if changed_keys.is_empty() {
            debug!("no routing changes detected");
            return Ok(None);
        }

        info!(changed = ?changed_keys, "routing changes detected");

        let result = match self.mode {
            RouterMode::Edge => {
                self.sync_edge_state(current_snapshot, &manifest, &changed_keys, hop_manager)
                    .await
            }
            RouterMode::Zone => {
                self.sync_zone_state(current_snapshot, &manifest, &changed_keys, hop_manager)
                    .await
            }
        };

        // Retire hops that are no longer in the routing table
        if let Ok(Some(ref snapshot)) = result {
            let active_ids = snapshot.get_all_hop_ids();
            hop_manager.retire_unused(&active_ids);
        }

        result
    }

    // -----------------------------------------------------------------------
    // Change Detection
    // -----------------------------------------------------------------------

    async fn detect_changes(&self, manifest: &VersionManifest) -> HashSet<String> {
        let mut changed = HashSet::new();
        let local = self.local_versions.read().await;

        for (key, version) in &manifest.versions {
            if local.get(key) != Some(version) {
                changed.insert(key.clone());
            }
        }

        changed
    }

    async fn update_local_version(&self, key: &str, version: u64) {
        let mut local = self.local_versions.write().await;
        local.insert(key.to_string(), version);
    }

    // -----------------------------------------------------------------------
    // Edge Sync
    // -----------------------------------------------------------------------

    async fn sync_edge_state(
        &self,
        current: Option<&RoutingSnapshot>,
        manifest: &VersionManifest,
        changed: &HashSet<String>,
        hop_manager: &HopManager,
    ) -> Result<Option<RoutingSnapshot>, RZError> {
        let mut state = match current {
            Some(RoutingSnapshot::Edge(s)) => s.clone(),
            _ => EdgeState::new(),
        };

        // Extract zone names from manifest keys
        let zone_keys: HashSet<String> = manifest
            .versions
            .keys()
            .filter(|k| k.starts_with("zones/"))
            .filter_map(|k| k.split('/').nth(1))
            .map(|s| s.to_string())
            .collect();

        for zone_id in &zone_keys {
            // Check if routers changed
            let routers_key = format!("zones/{}/routers", zone_id);
            if changed.contains(&routers_key) {
                match self.fetch_zone_routers(zone_id).await {
                    Ok((version, routers)) => {
                        let hop_ids: Vec<String> = routers
                            .into_iter()
                            .map(|id| {
                                hop_manager.get_or_create(&id);
                                id
                            })
                            .collect();
                        state.set_zone_routers(zone_id, hop_ids);
                        self.update_local_version(&routers_key, version).await;
                        debug!(zone = %zone_id, "updated zone routers");
                    }
                    Err(e) => {
                        warn!(%zone_id, error=%e, "failed to fetch zone routers, keeping old");
                    }
                }
            }

            // Check if segments changed
            let segments_key = format!("zones/{}/segments", zone_id);
            if changed.contains(&segments_key) {
                match self.fetch_zone_segments(zone_id).await {
                    Ok((version, segments)) => {
                        for seg in segments {
                            state.set_segment_zone(seg, zone_id.clone());
                        }
                        self.update_local_version(&segments_key, version).await;
                        debug!(zone = %zone_id, "updated zone segments");
                    }
                    Err(e) => {
                        warn!(%zone_id, error=%e, "failed to fetch zone segments, keeping old");
                    }
                }
            }
        }

        // Remove segments for zones that no longer exist
        state.retain_valid_zones(&zone_keys);

        if state.is_empty() {
            return Err(RZError::NoRoute("empty edge state after sync".into()));
        }

        Ok(Some(RoutingSnapshot::Edge(state)))
    }

    // -----------------------------------------------------------------------
    // Zone Sync
    // -----------------------------------------------------------------------

    async fn sync_zone_state(
        &self,
        current: Option<&RoutingSnapshot>,
        manifest: &VersionManifest,
        changed: &HashSet<String>,
        hop_manager: &HopManager,
    ) -> Result<Option<RoutingSnapshot>, RZError> {
        let mut state = match current {
            Some(RoutingSnapshot::Zone(s)) => s.clone(),
            _ => ZoneState::new(),
        };

        // Get shard topology for our zone
        let shards_key = format!("zones/{}/shards", self.zone_id);
        if changed.contains(&shards_key) || state.shard_to_bridges.is_empty() {
            match self.fetch_zone_shards(&self.zone_id).await {
                Ok((version, shards)) => {
                    for (shard_id, bridges) in shards {
                        let hop_ids: Vec<String> = bridges
                            .into_iter()
                            .map(|id| {
                                hop_manager.get_or_create(&id);
                                id
                            })
                            .collect();
                        state.set_shard_bridges(&shard_id, hop_ids);
                    }
                    self.update_local_version(&shards_key, version).await;
                    debug!(zone = %self.zone_id, "updated zone shards");
                }
                Err(e) => {
                    warn!(error=%e, "failed to fetch zone shards, keeping old");
                }
            }
        }

        // Extract shard IDs from manifest
        let shard_keys: HashSet<String> = manifest
            .versions
            .keys()
            .filter(|k| k.starts_with("shards/"))
            .filter_map(|k| k.split('/').nth(1))
            .map(|s| s.to_string())
            .collect();

        // Fetch segments for each shard
        for shard_id in &shard_keys {
            let segments_key = format!("shards/{}/segments", shard_id);
            if changed.contains(&segments_key) {
                match self.fetch_shard_segments(shard_id).await {
                    Ok((version, zone, segments)) => {
                        if zone != self.zone_id {
                            // Not our zone, skip
                            continue;
                        }
                        for seg in segments {
                            state.set_segment_shard(seg, shard_id.clone());
                        }
                        self.update_local_version(&segments_key, version).await;
                        debug!(shard = %shard_id, "updated shard segments");
                    }
                    Err(e) => {
                        warn!(%shard_id, error=%e, "failed to fetch shard segments, keeping old");
                    }
                }
            }
        }

        if state.is_empty() {
            return Err(RZError::NoRoute("empty zone state after sync".into()));
        }

        Ok(Some(RoutingSnapshot::Zone(state)))
    }
}
