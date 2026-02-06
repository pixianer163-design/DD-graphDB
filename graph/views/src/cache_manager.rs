//! Multi-Level Cache Manager for Materialized Views

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::path::PathBuf;

#[cfg(feature = "serde")]
use serde::{Serialize, Deserialize};

use graph_core::{VertexId, Edge, PropertyValue, Properties};

use super::multilevel_cache::{
    Cache, CacheConfig, ReplacementPolicy,
    MultiLevelCacheStats, create_cache
};

/// View cache data types
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ViewCacheData {
    VertexLookup { vertices: HashMap<VertexId, Properties> },
    Aggregation { result: PropertyValue, metadata: HashMap<String, PropertyValue> },
    Analytics { algorithm: String, result: PropertyValue, metadata: HashMap<String, PropertyValue> },
    EdgeTraversal { edges: Vec<(Edge, Properties)>, paths: Vec<Vec<VertexId>> },
    Serialized { data: Vec<u8>, compression: Option<String>, checksum: Option<u64> },
}

impl ViewCacheData {
    pub fn size_bytes(&self) -> usize {
        match self {
            ViewCacheData::VertexLookup { vertices } => vertices.len() * 64,
            ViewCacheData::Aggregation { metadata, .. } => 32 + metadata.len() * 32,
            ViewCacheData::Analytics { metadata, .. } => 132 + metadata.len() * 32,
            ViewCacheData::EdgeTraversal { edges, paths } => edges.len() * 64 + paths.len() * 50,
            ViewCacheData::Serialized { data, .. } => data.len(),
        }
    }

    pub fn compress_if_beneficial(self) -> ViewCacheData {
        self
    }
}

/// Cache level configuration
#[derive(Debug, Clone)]
pub struct CacheLevelConfig {
    pub cache_type: ReplacementPolicy,
    pub max_size_bytes: usize,
    pub default_ttl: Duration,
    pub enable_compression: bool,
    pub priority_threshold: f64,
}

impl Default for CacheLevelConfig {
    fn default() -> Self {
        Self {
            cache_type: ReplacementPolicy::Adaptive,
            max_size_bytes: 10 * 1024 * 1024,
            default_ttl: Duration::from_secs(300),
            enable_compression: false,
            priority_threshold: 50.0,
        }
    }
}

/// Multi-level cache configuration
#[derive(Debug, Clone)]
pub struct MultiLevelCacheConfig {
    pub l1_config: CacheLevelConfig,
    pub l2_config: CacheLevelConfig,
    pub l3_config: CacheLevelConfig,
    pub l3_disk_path: Option<PathBuf>,
    pub cleanup_interval: Duration,
    pub enable_adaptive_placement: bool,
}

impl Default for MultiLevelCacheConfig {
    fn default() -> Self {
        Self {
            l1_config: CacheLevelConfig {
                max_size_bytes: 50 * 1024 * 1024,
                default_ttl: Duration::from_secs(60),
                enable_compression: false,
                priority_threshold: 80.0,
                cache_type: ReplacementPolicy::LRU,
            },
            l2_config: CacheLevelConfig {
                max_size_bytes: 200 * 1024 * 1024,
                default_ttl: Duration::from_secs(300),
                enable_compression: true,
                priority_threshold: 40.0,
                cache_type: ReplacementPolicy::Adaptive,
            },
            l3_config: CacheLevelConfig {
                max_size_bytes: usize::MAX,
                default_ttl: Duration::from_secs(3600),
                enable_compression: true,
                priority_threshold: 0.0,
                cache_type: ReplacementPolicy::LFU,
            },
            l3_disk_path: Some(PathBuf::from("./cache_l3")),
            cleanup_interval: Duration::from_secs(60),
            enable_adaptive_placement: true,
        }
    }
}

/// Cache entry with placement metadata
#[derive(Debug, Clone)]
pub struct ViewCacheEntry {
    pub view_id: String,
    pub query_hash: u64,
    pub data: ViewCacheData,
    pub priority_score: f64,
    pub current_level: u8,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub access_count: u64,
    pub recompute_cost_ms: u64,
    pub dependencies: HashSet<String>,
}

impl ViewCacheEntry {
    pub fn new(
        view_id: String,
        query_hash: u64,
        data: ViewCacheData,
        priority_score: f64,
        recompute_cost_ms: u64,
    ) -> Self {
        let now = Instant::now();
        Self {
            view_id,
            query_hash,
            data,
            priority_score,
            current_level: 2,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            recompute_cost_ms,
            dependencies: HashSet::new(),
        }
    }

    pub fn access(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
        let age_minutes = self.created_at.elapsed().as_secs_f64() / 60.0;
        let access_rate = self.access_count as f64 / age_minutes.max(1.0);
        self.priority_score = (access_rate * 10.0).min(50.0)
            + ((self.access_count as f64).log10() * 10.0).min(30.0)
            + ((self.recompute_cost_ms as f64 / 1000.0) * 10.0).min(20.0);
    }

    pub fn target_level(&self) -> u8 {
        if self.priority_score >= 80.0 { 1 }
        else if self.priority_score >= 40.0 { 2 }
        else { 3 }
    }

    pub fn needs_level_change(&self) -> bool {
        self.target_level() != self.current_level
    }
}

/// Multi-level cache manager
pub struct MultiLevelCacheManager {
    config: MultiLevelCacheConfig,
    l1_cache: Arc<RwLock<Box<dyn Cache<String, ViewCacheEntry>>>>,
    l2_cache: Arc<RwLock<Box<dyn Cache<String, ViewCacheEntry>>>>,
    l3_cache: Arc<RwLock<Box<dyn Cache<String, ViewCacheEntry>>>>,
}

impl std::fmt::Debug for MultiLevelCacheManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiLevelCacheManager")
            .field("config", &self.config)
            .finish()
    }
}

impl MultiLevelCacheManager {
    pub fn new(config: MultiLevelCacheConfig) -> Self {
        let l1_cache = create_cache(
            config.l1_config.cache_type.clone(),
            CacheConfig {
                max_size_bytes: config.l1_config.max_size_bytes,
                replacement_policy: config.l1_config.cache_type.clone(),
                default_ttl: config.l1_config.default_ttl,
                cleanup_interval: config.cleanup_interval,
                hot_threshold: 80.0,
            },
        );
        let l2_cache = create_cache(
            config.l2_config.cache_type.clone(),
            CacheConfig {
                max_size_bytes: config.l2_config.max_size_bytes,
                replacement_policy: config.l2_config.cache_type.clone(),
                default_ttl: config.l2_config.default_ttl,
                cleanup_interval: config.cleanup_interval,
                hot_threshold: 50.0,
            },
        );
        let l3_cache = create_cache(
            config.l3_config.cache_type.clone(),
            CacheConfig {
                max_size_bytes: config.l3_config.max_size_bytes,
                replacement_policy: config.l3_config.cache_type.clone(),
                default_ttl: config.l3_config.default_ttl,
                cleanup_interval: config.cleanup_interval,
                hot_threshold: 20.0,
            },
        );
        Self {
            config,
            l1_cache: Arc::new(RwLock::new(l1_cache)),
            l2_cache: Arc::new(RwLock::new(l2_cache)),
            l3_cache: Arc::new(RwLock::new(l3_cache)),
        }
    }

    pub fn get(&self, view_id: &str, query_hash: u64) -> Option<ViewCacheData> {
        let cache_key = format!("{}:{}", view_id, query_hash);

        // Extract result first so the write guard is dropped before re-acquiring
        let l1_result = self.l1_cache.write().unwrap().get(&cache_key);
        if let Some(mut entry) = l1_result {
            entry.access();
            let data = entry.data.clone();
            let size = entry.data.size_bytes();
            self.l1_cache.write().unwrap().put(cache_key, entry, size);
            return Some(data);
        }

        let l2_result = self.l2_cache.write().unwrap().get(&cache_key);
        if let Some(mut entry) = l2_result {
            entry.access();
            let data = entry.data.clone();
            let size = entry.data.size_bytes();
            self.l2_cache.write().unwrap().put(cache_key, entry, size);
            return Some(data);
        }

        let l3_result = self.l3_cache.write().unwrap().get(&cache_key);
        if let Some(mut entry) = l3_result {
            entry.access();
            let data = entry.data.clone();
            let size = entry.data.size_bytes();
            self.l3_cache.write().unwrap().put(cache_key, entry, size);
            return Some(data);
        }

        None
    }

    pub fn put(&self, view_id: &str, query_hash: u64, data: ViewCacheData, recompute_cost_ms: u64) {
        let cache_key = format!("{}:{}", view_id, query_hash);
        let priority_score = self.calculate_initial_priority(&data, recompute_cost_ms);
        let entry = ViewCacheEntry::new(
            view_id.to_string(), query_hash, data, priority_score, recompute_cost_ms,
        );
        let target_level = if self.config.enable_adaptive_placement {
            entry.target_level()
        } else {
            2
        };
        let size = entry.data.size_bytes();
        match target_level {
            1 => { self.l1_cache.write().unwrap().put(cache_key, entry, size); }
            2 => { self.l2_cache.write().unwrap().put(cache_key, entry, size); }
            3 => { self.l3_cache.write().unwrap().put(cache_key, entry, size); }
            _ => {}
        }
    }

    pub fn remove(&self, view_id: &str, query_hash: u64) {
        let cache_key = format!("{}:{}", view_id, query_hash);
        self.l1_cache.write().unwrap().remove(&cache_key);
        self.l2_cache.write().unwrap().remove(&cache_key);
        self.l3_cache.write().unwrap().remove(&cache_key);
    }

    pub fn clear_all(&self) {
        self.l1_cache.write().unwrap().clear();
        self.l2_cache.write().unwrap().clear();
        self.l3_cache.write().unwrap().clear();
    }

    pub fn get_stats(&self) -> MultiLevelCacheStats {
        let l1_stats = self.l1_cache.read().unwrap().stats();
        let l2_stats = self.l2_cache.read().unwrap().stats();
        let l3_stats = self.l3_cache.read().unwrap().stats();
        MultiLevelCacheStats::new(l1_stats, l2_stats, l3_stats)
    }

    fn calculate_initial_priority(&self, data: &ViewCacheData, recompute_cost_ms: u64) -> f64 {
        let cost_priority = (recompute_cost_ms as f64 / 1000.0 * 20.0).min(40.0);
        let size_factor = match data.size_bytes() {
            0..=1024 => 30.0,
            1025..=10240 => 20.0,
            10241..=102400 => 10.0,
            _ => 5.0,
        };
        let type_factor = match data {
            ViewCacheData::VertexLookup { .. } => 25.0,
            ViewCacheData::Aggregation { .. } => 20.0,
            ViewCacheData::Analytics { .. } => 15.0,
            ViewCacheData::EdgeTraversal { .. } => 10.0,
            ViewCacheData::Serialized { .. } => 5.0,
        };
        cost_priority + size_factor + type_factor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_view_cache_data_sizing() {
        let data = ViewCacheData::Serialized {
            data: vec![1, 2, 3, 4, 5],
            compression: None,
            checksum: Some(5),
        };
        assert_eq!(data.size_bytes(), 5);
    }

    #[test]
    fn test_cache_level_placement() {
        let manager = MultiLevelCacheManager::new(MultiLevelCacheConfig::default());
        let data = ViewCacheData::VertexLookup { vertices: HashMap::new() };
        manager.put("test", 1, data, 5000);
        assert!(manager.get("test", 1).is_some());
    }

    #[test]
    fn test_cache_statistics() {
        let manager = MultiLevelCacheManager::new(MultiLevelCacheConfig::default());
        let data = ViewCacheData::VertexLookup { vertices: HashMap::new() };
        manager.put("test", 1, data, 1000);
        let stats = manager.get_stats();
        assert_eq!(stats.total_memory_usage, 0);
    }
}
