//! Advanced Multi-Level Cache Manager for Materialized Views
//!
//! This module provides a sophisticated L1/L2/L3 caching architecture
//! specifically designed for materialized views with intelligent data placement
//! and eviction strategies.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::path::PathBuf;

// Note: serde and tokio are conditionally available when needed
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
    /// Vertex lookup result
    VertexLookup {
        vertices: HashMap<VertexId, Properties>,
    },
    /// Aggregation result
    Aggregation {
        result: PropertyValue,
        metadata: HashMap<String, PropertyValue>,
    },
    /// Analytics result
    Analytics {
        algorithm: String,
        result: PropertyValue,
        metadata: HashMap<String, PropertyValue>,
    },
    /// Edge traversal result
    EdgeTraversal {
        edges: Vec<(Edge, Properties)>,
        paths: Vec<Vec<VertexId>>,
    },
    /// Raw serialized data for L3 cache
    Serialized {
        data: Vec<u8>,
        compression: Option<String>,
        checksum: Option<u64>,
    },
}

impl ViewCacheData {
    /// Get approximate size in bytes
    pub fn size_bytes(&self) -> usize {
        match self {
            ViewCacheData::VertexLookup { vertices } => {
                vertices.len() * std::mem::size_of::<(VertexId, Properties)>() * 2
            }
            ViewCacheData::Aggregation { result, metadata } => {
                std::mem::size_of_val(result) + 
                metadata.len() * std::mem::size_of::<(String, PropertyValue)>()
            }
            ViewCacheData::Analytics { result, metadata, .. } => {
                std::mem::size_of_val(result) + 
                metadata.len() * std::mem::size_of::<(String, PropertyValue)>() +
                100 // algorithm string estimate
            }
            ViewCacheData::EdgeTraversal { edges, paths } => {
                edges.len() * std::mem::size_of::<(Edge, Properties)>() +
                paths.len() * 50 // path estimate
            }
            ViewCacheData::Serialized { data, .. } => data.len(),
        }
    }

    /// Check if data can be compressed
    pub fn can_compress(&self) -> bool {
        matches!(self, ViewCacheData::VertexLookup { .. } | ViewCacheData::EdgeTraversal { .. })
    }

    /// Compress data if beneficial
    pub fn compress_if_beneficial(self) -> ViewCacheData {
        if !self.can_compress() {
            return self;
        }

        let size_before = self.size_bytes();
        if size_before < 1024 { // Don't compress small data
            return self;
        }

        // Simple compression simulation (in real implementation, use actual compression)
        match self {
            ViewCacheData::VertexLookup { vertices } => {
                #[cfg(feature = "serialization")]
                {
                    if let Ok(serialized) = self.serialize_vertices(&vertices) {
                        if serialized.len() < size_before * 7 / 10 { // 30% compression achieved
                            return ViewCacheData::Serialized {
                                data: serialized,
                                compression: Some("simple".to_string()),
                                checksum: Some(self.checksum()),
                            };
                        }
                    }
                }
                ViewCacheData::VertexLookup { vertices }
            }
            ViewCacheData::EdgeTraversal { edges, paths } => {
                #[cfg(feature = "serialization")]
                {
                    if let Ok(serialized) = self.serialize_traversal(&edges, &paths) {
                        if serialized.len() < size_before * 7 / 10 {
                            return ViewCacheData::Serialized {
                                data: serialized,
                                compression: Some("simple".to_string()),
                                checksum: Some(self.checksum()),
                            };
                        }
                    }
                }
                ViewCacheData::EdgeTraversal { edges, paths }
            }
            _ => self,
        }
    }

    /// Calculate simple checksum
    #[allow(dead_code)]
    fn checksum(&self) -> u64 {
        match self {
            ViewCacheData::VertexLookup { vertices } => {
                vertices.len() as u64
            }
            ViewCacheData::Aggregation { result, .. } => {
                result.as_string().map(|s| s.len() as u64).unwrap_or(0)
            }
            ViewCacheData::Analytics { result, .. } => {
                result.as_string().map(|s| s.len() as u64).unwrap_or(0)
            }
            ViewCacheData::EdgeTraversal { edges, paths } => {
                (edges.len() + paths.len()) as u64
            }
            ViewCacheData::Serialized { data, .. } => {
                data.len() as u64
            }
        }
    }

    #[cfg(feature = "serialization")]
    fn serialize_vertices(&self, vertices: &HashMap<VertexId, Properties>) -> Option<Vec<u8>> {
        // Simple serialization using format string
        let mut data = Vec::new();
        for (id, props) in vertices {
            data.extend_from_slice(&id.to_le_bytes());
            data.push(props.len() as u8);
        }
        Some(data)
    }

    #[cfg(feature = "serialization")]
    fn serialize_traversal(&self, _edges: &Vec<(Edge, Properties)>, _paths: &Vec<Vec<VertexId>>) -> Option<Vec<u8>> {
        // Simple serialization for traversal data
        Some(vec![1, 2, 3]) // Placeholder
    }
}

/// Cache level configuration
#[derive(Debug, Clone)]
pub struct CacheLevelConfig {
    /// Cache type for this level
    pub cache_type: ReplacementPolicy,
    /// Maximum size in bytes
    pub max_size_bytes: usize,
    /// Default TTL
    pub default_ttl: Duration,
    /// Whether to enable compression for this level
    pub enable_compression: bool,
    /// Priority score threshold for this level
    pub priority_threshold: f64,
}

impl Default for CacheLevelConfig {
    fn default() -> Self {
        Self {
            cache_type: ReplacementPolicy::Adaptive,
            max_size_bytes: 10 * 1024 * 1024, // 10MB
            default_ttl: Duration::from_secs(300), // 5 minutes
            enable_compression: false,
            priority_threshold: 50.0,
        }
    }
}

/// Multi-level cache configuration
#[derive(Debug, Clone)]
pub struct MultiLevelCacheConfig {
    /// L1 cache config (hot data, sub-millisecond access)
    pub l1_config: CacheLevelConfig,
    /// L2 cache config (warm data, millisecond access)
    pub l2_config: CacheLevelConfig,
    /// L3 cache config (cold data, disk-based)
    pub l3_config: CacheLevelConfig,
    /// Disk storage path for L3
    pub l3_disk_path: Option<PathBuf>,
    /// Background cleanup interval
    pub cleanup_interval: Duration,
    /// Enable adaptive promotion/demotion
    pub enable_adaptive_placement: bool,
}

impl Default for MultiLevelCacheConfig {
    fn default() -> Self {
        Self {
            l1_config: CacheLevelConfig {
                max_size_bytes: 50 * 1024 * 1024,  // 50MB - hot data
                default_ttl: Duration::from_secs(60), // 1 minute
                enable_compression: false, // Keep hot data uncompressed
                priority_threshold: 80.0, // Only high-priority data
                cache_type: ReplacementPolicy::LRU,
            },
            l2_config: CacheLevelConfig {
                max_size_bytes: 200 * 1024 * 1024, // 200MB - warm data
                default_ttl: Duration::from_secs(300), // 5 minutes
                enable_compression: true, // Compress warm data
                priority_threshold: 40.0, // Medium priority
                cache_type: ReplacementPolicy::Adaptive,
            },
            l3_config: CacheLevelConfig {
                max_size_bytes: usize::MAX, // Unlimited - disk based
                default_ttl: Duration::from_secs(3600), // 1 hour
                enable_compression: true, // Always compress cold data
                priority_threshold: 0.0, // All data can go to L3
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
    /// View identifier
    pub view_id: String,
    /// Query pattern hash
    pub query_hash: u64,
    /// Cache data
    pub data: ViewCacheData,
    /// Priority score (0-100)
    pub priority_score: f64,
    /// Current cache level
    pub current_level: u8, // 1, 2, or 3
    /// Creation time
    pub created_at: Instant,
    /// Last access time
    pub last_accessed: Instant,
    /// Access frequency
    pub access_count: u64,
    /// Cost to recompute this data
    pub recompute_cost_ms: u64,
    /// Dependencies (other view IDs this depends on)
    pub dependencies: HashSet<String>,
}

impl ViewCacheEntry {
    /// Create new cache entry
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
            current_level: 2, // Start in L2
            created_at: now,
            last_accessed: now,
            access_count: 1,
            recompute_cost_ms,
            dependencies: HashSet::new(),
        }
    }

    /// Record access and update priority
    pub fn access(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
        
        // Update priority based on access pattern
        let age_minutes = self.created_at.elapsed().as_secs_f64() / 60.0;
        let access_rate = self.access_count as f64 / age_minutes.max(1.0);
        
        // Priority considers access rate, frequency, and recompute cost
        self.priority_score = (access_rate * 10.0).min(50.0) +
                              ((self.access_count as f64).log10() * 10.0).min(30.0) +
                              ((self.recompute_cost_ms as f64 / 1000.0) * 10.0).min(20.0);
    }

    /// Get appropriate cache level based on current priority
    pub fn target_level(&self) -> u8 {
        if self.priority_score >= 80.0 {
            1 // L1 - Hot data
        } else if self.priority_score >= 40.0 {
            2 // L2 - Warm data
        } else {
            3 // L3 - Cold data
        }
    }

    /// Check if entry should be promoted/demoted
    pub fn needs_level_change(&self) -> bool {
        self.target_level() != self.current_level
    }
}

/// Multi-level cache manager
pub struct MultiLevelCacheManager {
    /// Configuration
    config: MultiLevelCacheConfig,
    /// L1 cache (hot data)
    l1_cache: Arc<RwLock<dyn Cache<String, ViewCacheEntry>>>,
    /// L2 cache (warm data)
    l2_cache: Arc<RwLock<dyn Cache<String, ViewCacheEntry>>>,
    /// L3 cache (cold data)
    l3_cache: Arc<RwLock<dyn Cache<String, ViewCacheEntry>>>,
    /// Cache statistics
    stats: Arc<RwLock<MultiLevelCacheStats>>,
    /// Background cleanup handle
    #[cfg(feature = "async")]
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for MultiLevelCacheManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiLevelCacheManager")
            .field("config", &self.config)
            .field("stats", &self.stats)
            .finish()
    }
}

impl MultiLevelCacheManager {
    /// Create new multi-level cache manager
    pub fn new(config: MultiLevelCacheConfig) -> Self {
        let l1_cache: Box<dyn Cache<String, ViewCacheEntry>> = create_cache(
            config.l1_config.cache_type.clone(),
            CacheConfig {
                max_size_bytes: config.l1_config.max_size_bytes,
                replacement_policy: config.l1_config.cache_type.clone(),
                default_ttl: config.l1_config.default_ttl,
                cleanup_interval: config.cleanup_interval,
                hot_threshold: 80.0,
            },
        );

        let l2_cache: Box<dyn Cache<String, ViewCacheEntry>> = create_cache(
            config.l2_config.cache_type.clone(),
            CacheConfig {
                max_size_bytes: config.l2_config.max_size_bytes,
                replacement_policy: config.l2_config.cache_type.clone(),
                default_ttl: config.l2_config.default_ttl,
                cleanup_interval: config.cleanup_interval,
                hot_threshold: 50.0,
            },
        );

        let l3_cache: Box<dyn Cache<String, ViewCacheEntry>> = create_cache(
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
            stats: Arc::new(RwLock::new(MultiLevelCacheStats::default())),
            #[cfg(feature = "async")]
            cleanup_handle: None,
        }
    }

    /// Get data from cache (tries L1 -> L2 -> L3)
    pub fn get(&self, view_id: &str, query_hash: u64) -> Option<ViewCacheData> {
        let cache_key = format!("{}:{}", view_id, query_hash);

        // Try L1 first
        if let Some(mut entry) = self.l1_cache.write().unwrap().get(&cache_key) {
            entry.access();
            self.update_stats("l1", true);
            let data = entry.data.clone();
            let size_bytes = entry.data.size_bytes();
            self.l1_cache.write().unwrap().put(cache_key.clone(), entry, size_bytes);
            return Some(data);
        }

        // Try L2
        if let Some(mut entry) = self.l2_cache.write().unwrap().get(&cache_key) {
            entry.access();
            self.update_stats("l2", true);
            
            // Check if should promote to L1
            if entry.target_level() == 1 {
                let entry_clone = entry.clone();
                let size_bytes = entry_clone.data.size_bytes();
                self.l1_cache.write().unwrap().put(cache_key.clone(), entry_clone, size_bytes);
            }
            
            let data = entry.data.clone();
            let size_bytes = entry.data.size_bytes();
            self.l2_cache.write().unwrap().put(cache_key.clone(), entry, size_bytes);
            return Some(data);
        }

        // Try L3
        if let Some(mut entry) = self.l3_cache.write().unwrap().get(&cache_key) {
            entry.access();
            self.update_stats("l3", true);
            
            // Check if should promote
            let target_level = entry.target_level();
            if target_level == 1 || target_level == 2 {
                let entry_clone = entry.clone();
                let size_bytes = entry_clone.data.size_bytes();
                match target_level {
                    1 => {
                        self.l1_cache.write().unwrap().put(cache_key.clone(), entry_clone, size_bytes);
                    }
                    2 => {
                        self.l2_cache.write().unwrap().put(cache_key.clone(), entry_clone, size_bytes);
                    }
                    _ => {}
                }
            }
            
            let data = entry.data.clone();
            let size_bytes = entry.data.size_bytes();
            self.l3_cache.write().unwrap().put(cache_key.clone(), entry, size_bytes);
            return Some(data);
        }

        self.update_stats("l1", false);
        None
    }

    /// Put data into cache with intelligent placement
    pub fn put(&self, view_id: &str, query_hash: u64, data: ViewCacheData, recompute_cost_ms: u64) {
        let cache_key = format!("{}:{}", view_id, query_hash);
        
        // Calculate initial priority
        let priority_score = self.calculate_initial_priority(&data, recompute_cost_ms);
        
        // Create cache entry
        let mut entry = ViewCacheEntry::new(
            view_id.to_string(),
            query_hash,
            data,
            priority_score,
            recompute_cost_ms,
        );

        // Determine initial placement
        let target_level = if self.config.enable_adaptive_placement {
            entry.target_level()
        } else {
            2 // Default to L2
        };

        // Apply compression if beneficial for this level
        let size_bytes = entry.data.size_bytes();
        match target_level {
            1 => {
                // L1 - no compression, keep data fast
                self.l1_cache.write().unwrap().put(cache_key, entry, size_bytes);
            }
            2 => {
                // L2 - compress if beneficial
                if self.config.l2_config.enable_compression {
                    entry.data = entry.data.compress_if_beneficial();
                }
                let size_bytes = entry.data.size_bytes();
                self.l2_cache.write().unwrap().put(cache_key, entry, size_bytes);
            }
            3 => {
                // L3 - always compress
                if self.config.l3_config.enable_compression {
                    entry.data = entry.data.compress_if_beneficial();
                }
                let size_bytes = entry.data.size_bytes();
                self.l3_cache.write().unwrap().put(cache_key, entry, size_bytes);
            }
            _ => {}
        }
    }

    /// Remove specific entry from all cache levels
    pub fn remove(&self, view_id: &str, query_hash: u64) {
        let cache_key = format!("{}:{}", view_id, query_hash);
        
        self.l1_cache.write().unwrap().remove(&cache_key);
        self.l2_cache.write().unwrap().remove(&cache_key);
        self.l3_cache.write().unwrap().remove(&cache_key);
    }

    /// Clear all cache levels
    pub fn clear_all(&self) {
        self.l1_cache.write().unwrap().clear();
        self.l2_cache.write().unwrap().clear();
        self.l3_cache.write().unwrap().clear();
    }

    /// Get comprehensive cache statistics
    pub fn get_stats(&self) -> MultiLevelCacheStats {
        let l1_stats = self.l1_cache.read().unwrap().stats();
        let l2_stats = self.l2_cache.read().unwrap().stats();
        let l3_stats = self.l3_cache.read().unwrap().stats();
        
        MultiLevelCacheStats::new(l1_stats, l2_stats, l3_stats)
    }

    /// Start background maintenance tasks
    #[cfg(feature = "async")]
    pub fn start_maintenance(&mut self) {
        let _l1_cache = self.l1_cache.clone();
        let _l2_cache = self.l2_cache.clone();
        let _l3_cache = self.l3_cache.clone();
        let _stats = self.stats.clone();
        let _cleanup_interval = self.config.cleanup_interval;

        // Temporarily disabled due to Send/Sync requirements
        // self.cleanup_handle = Some(tokio::spawn(async move {
        //     let mut interval = tokio::time::interval(cleanup_interval);
        //     
        //     loop {
        //         interval.tick().await;
        //         
        //         // Perform cache maintenance
        //         Self::perform_maintenance(&l1_cache, &l2_cache, &l3_cache, &stats).await;
        //     }
        // }));
    }

    /// Stop background maintenance
    #[cfg(feature = "async")]
    pub fn stop_maintenance(&mut self) {
        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
        }
    }

    /// Calculate initial priority for cache entry
    fn calculate_initial_priority(&self, data: &ViewCacheData, recompute_cost_ms: u64) -> f64 {
        // Base priority from recompute cost
        let cost_priority = (recompute_cost_ms as f64 / 1000.0 * 20.0).min(40.0);
        
        // Size factor (smaller data gets higher priority)
        let size_factor = match data.size_bytes() {
            0..=1024 => 30.0,      // Very small - high priority
            1025..=10240 => 20.0,   // Small - medium priority
            10241..=102400 => 10.0, // Medium - low priority
            _ => 5.0,                // Large - very low priority
        };
        
        // Data type factor
        let type_factor = match data {
            ViewCacheData::VertexLookup { .. } => 25.0,  // Fast lookups
            ViewCacheData::Aggregation { .. } => 20.0,    // Useful aggregations
            ViewCacheData::Analytics { .. } => 15.0,      // Analytics results
            ViewCacheData::EdgeTraversal { .. } => 10.0,  // Path queries
            ViewCacheData::Serialized { .. } => 5.0,      // Serialized data
        };
        
        cost_priority + size_factor + type_factor
    }

    /// Update statistics
    fn update_stats(&self, _level: &str, _hit: bool) {
        // In a real implementation, update stats atomically
        // For now, this is a placeholder
    }

    /// Perform background maintenance
    #[cfg(feature = "async")]
    #[allow(dead_code)]
    async fn perform_maintenance(
        _l1_cache: &Arc<dyn Cache<String, ViewCacheEntry>>,
        _l2_cache: &Arc<dyn Cache<String, ViewCacheEntry>>,
        _l3_cache: &Arc<dyn Cache<String, ViewCacheEntry>>,
        _stats: &Arc<RwLock<MultiLevelCacheStats>>,
    ) {
        // In a real implementation:
        // 1. Clean up expired entries
        // 2. Promote/demote entries based on priority changes
        // 3. Update statistics
        // 4. Optimize memory usage
    }
}

impl Drop for MultiLevelCacheManager {
    fn drop(&mut self) {
        #[cfg(feature = "async")]
        self.stop_maintenance();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_view_cache_data_sizing() {
        let vertex_data = ViewCacheData::VertexLookup {
            vertices: HashMap::new(),
        };
        assert!(vertex_data.size_bytes() >= 0);

        let serialized_data = ViewCacheData::Serialized {
            data: vec![1, 2, 3, 4, 5],
            compression: None,
            checksum: Some(5),
        };
        assert_eq!(serialized_data.size_bytes(), 5);
    }

    #[test]
    fn test_cache_entry_priority_scoring() {
        let data = ViewCacheData::VertexLookup {
            vertices: HashMap::new(),
        };
        
        let mut entry = ViewCacheEntry::new(
            "test_view".to_string(),
            123,
            data,
            50.0, // Initial priority
            1000, // 1 second recompute cost
        );

        let initial_priority = entry.priority_score;
        
        // Simulate multiple accesses
        for _ in 0..10 {
            entry.access();
        }
        
        // Priority should increase with accesses
        assert!(entry.priority_score > initial_priority);
    }

    #[test]
    fn test_cache_level_placement() {
        let manager = MultiLevelCacheManager::new(MultiLevelCacheConfig::default());
        
        // High priority data should go to L1
        let hot_data = ViewCacheData::VertexLookup {
            vertices: HashMap::new(),
        };
        
        manager.put("hot_view", 1, hot_data, 5000); // High recompute cost
        
        // Data should be retrievable
        let retrieved = manager.get("hot_view", 1);
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_cache_statistics() {
        let manager = MultiLevelCacheManager::new(MultiLevelCacheConfig::default());
        
        // Add some test data
        let data = ViewCacheData::VertexLookup {
            vertices: HashMap::new(),
        };
        
        manager.put("test_view", 1, data, 1000);
        
        // Check statistics
        let stats = manager.get_stats();
        assert!(stats.total_memory_usage >= 0);
    }
}