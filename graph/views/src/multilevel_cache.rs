//! High-Performance Multi-Level Cache for Materialized Views
//!
//! This module implements a sophisticated L1/L2/L3 caching system
//! designed for sub-millisecond query performance in materialized views.

use std::collections::{HashMap, VecDeque};
// VecDeque still used by AdaptiveCache

use std::time::{Duration, Instant};
use std::hash::Hash;




/// Cache entry with metadata
#[derive(Debug, Clone)]
pub struct CacheEntry<T> {
    /// Cached data
    pub data: T,
    /// Creation timestamp
    pub created_at: Instant,
    /// Last access timestamp
    pub last_accessed: Instant,
    /// Access frequency
    pub access_count: u64,
    /// Size in bytes
    pub size_bytes: usize,
    /// Time-to-live
    pub ttl: Duration,
}

impl<T> CacheEntry<T> {
    /// Create new cache entry
    pub fn new(data: T, size_bytes: usize, ttl: Duration) -> Self {
        let now = Instant::now();
        Self {
            data,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            size_bytes,
            ttl,
        }
    }

    /// Check if entry is expired
    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }

    /// Record access
    pub fn access(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }

    /// Get access frequency score
    pub fn frequency_score(&self) -> f64 {
        let age = self.created_at.elapsed().as_secs_f64();
        if age == 0.0 { return self.access_count as f64; }
        (self.access_count as f64) / age
    }
}

/// Cache replacement policy
#[derive(Debug, Clone, PartialEq)]
pub enum ReplacementPolicy {
    /// Least Recently Used
    LRU,
    /// Least Frequently Used
    LFU,
    /// Adaptive combination of LRU and LFU
    Adaptive,
}

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum size in bytes
    pub max_size_bytes: usize,
    /// Replacement policy
    pub replacement_policy: ReplacementPolicy,
    /// Default TTL for entries
    pub default_ttl: Duration,
    /// Cleanup interval
    pub cleanup_interval: Duration,
    /// Hot data threshold (for adaptive policies)
    pub hot_threshold: f64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 1024 * 1024, // 1MB
            replacement_policy: ReplacementPolicy::Adaptive,
            default_ttl: Duration::from_secs(300), // 5 minutes
            cleanup_interval: Duration::from_secs(60), // 1 minute
            hot_threshold: 5.0,
        }
    }
}

/// Generic cache interface
pub trait Cache<K: Hash + Eq, V: Clone>: Send + Sync {
    /// Get value from cache
    fn get(&mut self, key: &K) -> Option<V>;
    
    /// Put value in cache
    fn put(&mut self, key: K, value: V, size_bytes: usize) -> Option<()>;
    
    /// Remove value from cache
    fn remove(&mut self, key: &K) -> Option<V>;
    
    /// Get cache statistics
    fn stats(&self) -> CacheStats;
    
    /// Clear cache
    fn clear(&mut self);
    
    /// Get current size
    fn size_bytes(&self) -> usize;
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Total number of entries
    pub entries: usize,
    /// Hit count
    pub hits: u64,
    /// Miss count
    pub misses: u64,
    /// Eviction count
    pub evictions: u64,
    /// Current memory usage
    pub memory_usage_bytes: usize,
    /// Hit ratio
    pub hit_ratio: f64,
}

impl CacheStats {
    /// Update hit ratio
    pub fn update_hit_ratio(&mut self) {
        let total = self.hits + self.misses;
        if total > 0 {
            self.hit_ratio = self.hits as f64 / total as f64;
        }
    }
}

/// High-performance LRU cache with O(1) access-order tracking
#[derive(Debug)]
pub struct LruCache<K: Hash + Eq, V: Clone> {
    entries: HashMap<K, CacheEntry<V>>,
    /// Tracks access order using a monotonic counter. Higher = more recent.
    access_counter: HashMap<K, u64>,
    /// Monotonic counter for access ordering
    next_access_id: u64,
    config: CacheConfig,
    current_size: usize,
    stats: CacheStats,
}

impl<K: Hash + Eq + Clone, V: Clone> LruCache<K, V> {
    /// Create new LRU cache
    pub fn new(config: CacheConfig) -> Self {
        Self {
            entries: HashMap::new(),
            access_counter: HashMap::new(),
            next_access_id: 0,
            config,
            current_size: 0,
            stats: CacheStats::default(),
        }
    }

    /// Get value and update LRU order - O(1)
    pub fn get(&mut self, key: &K) -> Option<V> {
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.is_expired() {
                let removed = self.entries.remove(key);
                self.access_counter.remove(key);
                if let Some(removed_entry) = removed {
                    self.current_size = self.current_size.saturating_sub(removed_entry.size_bytes);
                }
                return None;
            }

            entry.access();
            self.access_counter.insert(key.clone(), self.next_access_id);
            self.next_access_id += 1;
            self.stats.hits += 1;
            Some(entry.data.clone())
        } else {
            self.stats.misses += 1;
            None
        }
    }

    /// Put value - O(1) amortized
    pub fn put(&mut self, key: K, value: V, size_bytes: usize) -> Option<()> {
        if self.current_size + size_bytes > self.config.max_size_bytes {
            self.evict_for_size(size_bytes);
        }

        // Remove existing entry
        if let Some(old) = self.entries.remove(&key) {
            self.current_size = self.current_size.saturating_sub(old.size_bytes);
        }

        let entry = CacheEntry::new(value, size_bytes, self.config.default_ttl);
        self.entries.insert(key.clone(), entry);
        self.access_counter.insert(key, self.next_access_id);
        self.next_access_id += 1;
        self.current_size += size_bytes;

        Some(())
    }

    /// Remove entry - O(1)
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.access_counter.remove(key);
        if let Some(entry) = self.entries.remove(key) {
            self.current_size = self.current_size.saturating_sub(entry.size_bytes);
            Some(entry.data)
        } else {
            None
        }
    }

    /// Clear cache
    pub fn clear(&mut self) {
        self.entries.clear();
        self.access_counter.clear();
        self.current_size = 0;
    }

    /// Get statistics
    pub fn stats(&self) -> CacheStats {
        let mut stats = self.stats.clone();
        stats.entries = self.entries.len();
        stats.memory_usage_bytes = self.current_size;
        stats.update_hit_ratio();
        stats
    }

    /// Get current size
    pub fn size_bytes(&self) -> usize {
        self.current_size
    }

    /// Evict least recently used entries to make room
    /// Finds minimum access_id (O(N) only during eviction, not on every access)
    fn evict_for_size(&mut self, needed_size: usize) {
        while self.current_size + needed_size > self.config.max_size_bytes {
            // Find the key with the smallest access_id (least recently used)
            let oldest_key = self.access_counter.iter()
                .min_by_key(|(_, &access_id)| access_id)
                .map(|(k, _)| k.clone());

            if let Some(key) = oldest_key {
                self.access_counter.remove(&key);
                if let Some(entry) = self.entries.remove(&key) {
                    self.current_size = self.current_size.saturating_sub(entry.size_bytes);
                    self.stats.evictions += 1;
                }
            } else {
                break;
            }
        }
    }
}

/// Adaptive cache combining LRU and LFU
#[derive(Debug)]
pub struct AdaptiveCache<K: Hash + Eq, V: Clone> {
    entries: HashMap<K, CacheEntry<V>>,
    hot_keys: VecDeque<K>,
    cold_keys: VecDeque<K>,
    config: CacheConfig,
    current_size: usize,
    stats: CacheStats,
}

impl<K: Hash + Eq + Clone, V: Clone> AdaptiveCache<K, V> {
    /// Create new adaptive cache
    pub fn new(config: CacheConfig) -> Self {
        Self {
            entries: HashMap::new(),
            hot_keys: VecDeque::new(),
            cold_keys: VecDeque::new(),
            config,
            current_size: 0,
            stats: CacheStats::default(),
        }
    }

    /// Get value with adaptive selection
    pub fn get(&mut self, key: &K) -> Option<V> {
        if let Some(mut entry) = self.entries.get(key).cloned() {
            if entry.is_expired() {
                self.remove(key);
                return None;
            }
            
            entry.access();
            self.update_hot_cold_status(key, &entry);
            self.stats.hits += 1;
            Some(entry.data)
        } else {
            self.stats.misses += 1;
            None
        }
    }

    /// Put value with adaptive placement
    pub fn put(&mut self, key: K, value: V, size_bytes: usize) -> Option<()> {
        let entry_size = size_bytes;
        if self.current_size + entry_size > self.config.max_size_bytes {
            self.adaptive_evict(entry_size);
        }

        // Remove existing
        if self.entries.contains_key(&key) {
            self.remove(&key);
        }

        // Add new entry
        let entry = CacheEntry::new(value, entry_size, self.config.default_ttl);
        self.entries.insert(key.clone(), entry);
        self.current_size += entry_size;
        
        // Initially place in cold list
        self.cold_keys.push_back(key);
        
        Some(())
    }

    /// Remove entry
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(entry) = self.entries.remove(key) {
            self.remove_from_queues(key);
            self.current_size = self.current_size.saturating_sub(entry.size_bytes);
            Some(entry.data)
        } else {
            None
        }
    }

    /// Clear cache
    pub fn clear(&mut self) {
        self.entries.clear();
        self.hot_keys.clear();
        self.cold_keys.clear();
        self.current_size = 0;
    }

    /// Get statistics
    pub fn stats(&self) -> CacheStats {
        let mut stats = self.stats.clone();
        stats.entries = self.entries.len();
        stats.memory_usage_bytes = self.current_size;
        stats.update_hit_ratio();
        stats
    }

    /// Get current size
    pub fn size_bytes(&self) -> usize {
        self.current_size
    }

    /// Update hot/cold status based on frequency
    fn update_hot_cold_status(&mut self, key: &K, entry: &CacheEntry<V>) {
        let frequency = entry.frequency_score();
        
        // Move from cold to hot if above threshold
        if frequency > self.config.hot_threshold {
            if !self.hot_keys.contains(&key) {
                self.remove_from_queues(key);
                self.hot_keys.push_back(key.clone());
            }
        }
        // Move from hot to cold if below threshold
        else if frequency < self.config.hot_threshold * 0.5 {
            if self.hot_keys.contains(&key) {
                self.remove_from_queues(key);
                self.cold_keys.push_back(key.clone());
            }
        }
    }

    /// Remove from both hot and cold queues
    fn remove_from_queues(&mut self, key: &K) {
        self.hot_keys.retain(|k| k != key);
        self.cold_keys.retain(|k| k != key);
    }

    /// Adaptive eviction strategy
    fn adaptive_evict(&mut self, needed_size: usize) {
        while self.current_size + needed_size > self.config.max_size_bytes {
            // Prefer evicting from cold list first
            if let Some(cold_key) = self.cold_keys.pop_front() {
                if let Some(entry) = self.entries.remove(&cold_key) {
                    self.current_size = self.current_size.saturating_sub(entry.size_bytes);
                    self.stats.evictions += 1;
                }
            }
            // Then try hot list
            else if let Some(hot_key) = self.hot_keys.pop_front() {
                if let Some(entry) = self.entries.remove(&hot_key) {
                    self.current_size = self.current_size.saturating_sub(entry.size_bytes);
                    self.stats.evictions += 1;
                }
            }
        }
    }
}

impl<K: Hash + Eq + Clone + Send + Sync, V: Clone + Send + Sync> Cache<K, V> for LruCache<K, V> {
    fn get(&mut self, key: &K) -> Option<V> {
        LruCache::get(self, key)
    }

    fn put(&mut self, key: K, value: V, size_bytes: usize) -> Option<()> {
        LruCache::put(self, key, value, size_bytes)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        LruCache::remove(self, key)
    }

    fn stats(&self) -> CacheStats {
        LruCache::stats(self)
    }

    fn clear(&mut self) {
        LruCache::clear(self)
    }

    fn size_bytes(&self) -> usize {
        LruCache::size_bytes(self)
    }
}

impl<K: Hash + Eq + Clone + Send + Sync, V: Clone + Send + Sync> Cache<K, V> for AdaptiveCache<K, V> {
    fn get(&mut self, key: &K) -> Option<V> {
        AdaptiveCache::get(self, key)
    }

    fn put(&mut self, key: K, value: V, size_bytes: usize) -> Option<()> {
        AdaptiveCache::put(self, key, value, size_bytes)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        AdaptiveCache::remove(self, key)
    }

    fn stats(&self) -> CacheStats {
        AdaptiveCache::stats(self)
    }

    fn clear(&mut self) {
        AdaptiveCache::clear(self)
    }

    fn size_bytes(&self) -> usize {
        AdaptiveCache::size_bytes(self)
    }
}

/// Cache factory
pub fn create_cache<K: Hash + Eq + Clone + Send + Sync + 'static, V: Clone + Send + Sync + 'static>(
    cache_type: ReplacementPolicy,
    config: CacheConfig,
) -> Box<dyn Cache<K, V>> {
    match cache_type {
        ReplacementPolicy::LRU => Box::new(LruCache::new(config)),
        ReplacementPolicy::LFU => {
            // For now, use LRU for LFU as well (can be extended)
            Box::new(LruCache::new(config))
        }
        ReplacementPolicy::Adaptive => Box::new(AdaptiveCache::new(config)),
    }
}

/// Multi-level cache statistics
#[derive(Debug, Clone, Default)]
pub struct MultiLevelCacheStats {
    /// L1 cache statistics
    pub l1_stats: CacheStats,
    /// L2 cache statistics
    pub l2_stats: CacheStats,
    /// L3 cache statistics
    pub l3_stats: CacheStats,
    /// Overall hit ratio
    pub overall_hit_ratio: f64,
    /// Total memory usage
    pub total_memory_usage: usize,
}

impl MultiLevelCacheStats {
    /// Create from individual stats
    pub fn new(l1: CacheStats, l2: CacheStats, l3: CacheStats) -> Self {
        let total_hits = l1.hits + l2.hits + l3.hits;
        let total_requests = total_hits + l1.misses + l2.misses + l3.misses;
        let overall_hit_ratio = if total_requests > 0 {
            total_hits as f64 / total_requests as f64
        } else {
            0.0
        };

        Self {
            l1_stats: l1.clone(),
            l2_stats: l2.clone(),
            l3_stats: l3.clone(),
            overall_hit_ratio,
            total_memory_usage: l1.memory_usage_bytes + l2.memory_usage_bytes + l3.memory_usage_bytes,
        }
    }

    /// Get pretty formatted summary
    pub fn summary(&self) -> String {
        format!(
            "Multi-Level Cache Summary:\n\
            ├─ L1 (Hot): {:.2}% hit rate, {} MB\n\
            ├─ L2 (Warm): {:.2}% hit rate, {} MB\n\
            └─ L3 (Cold): {:.2}% hit rate, {} MB\n\
            Overall: {:.2}% hit rate, {} MB total",
            self.l1_stats.hit_ratio * 100.0,
            self.l1_stats.memory_usage_bytes / (1024 * 1024),
            self.l2_stats.hit_ratio * 100.0,
            self.l2_stats.memory_usage_bytes / (1024 * 1024),
            self.l3_stats.hit_ratio * 100.0,
            self.l3_stats.memory_usage_bytes / (1024 * 1024),
            self.overall_hit_ratio * 100.0,
            self.total_memory_usage / (1024 * 1024)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_lru_cache_basic() {
        let config = CacheConfig {
            max_size_bytes: 1024,
            replacement_policy: ReplacementPolicy::LRU,
            default_ttl: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(10),
            hot_threshold: 5.0,
        };

        let mut cache: Box<dyn Cache<i32, String>> = create_cache(ReplacementPolicy::LRU, config);
        
        // Test put and get
        assert!(cache.put(1, "one".to_string(), 10).is_some());
        assert_eq!(cache.get(&1), Some("one".to_string()));
        assert_eq!(cache.get(&2), None);
        
        // Test LRU eviction
        for i in 2..=110 {
            let _ = cache.put(i, format!("value_{}", i), 10);
        }
        
        // First entry should be evicted
        assert_eq!(cache.get(&1), None);
        
        // Stats should show evictions
        let stats = cache.stats();
        assert!(stats.evictions > 0);
    }

    #[test]
    fn test_adaptive_cache_frequency() {
        let config = CacheConfig::default();
        let mut cache: Box<dyn Cache<i32, String>> = create_cache(ReplacementPolicy::Adaptive, config);
        
        // Add entries
        cache.put(1, "low_freq".to_string(), 10);
        cache.put(2, "high_freq".to_string(), 10);
        
        // Access high frequency entry multiple times
        for _ in 0..10 {
            let _ = cache.get(&2);
        }
        
        let stats = cache.stats();
        assert!(stats.hits >= 10);
    }

    #[test]
    fn test_cache_expiration() {
        let config = CacheConfig {
            max_size_bytes: 1024,
            replacement_policy: ReplacementPolicy::LRU,
            default_ttl: Duration::from_millis(100), // Short TTL
            cleanup_interval: Duration::from_secs(10),
            hot_threshold: 5.0,
        };

        let mut cache: Box<dyn Cache<i32, String>> = create_cache(ReplacementPolicy::LRU, config);
        
        cache.put(1, "expires".to_string(), 10);
        
        // Should be present immediately
        assert!(cache.get(&1).is_some());
        
        // Wait for expiration
        thread::sleep(Duration::from_millis(150));
        
        // Should be expired
        assert_eq!(cache.get(&1), None);
    }

    #[test]
    fn test_cache_size_limits() {
        let config = CacheConfig {
            max_size_bytes: 50, // Very small cache
            replacement_policy: ReplacementPolicy::LRU,
            default_ttl: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(10),
            hot_threshold: 5.0,
        };

        let mut cache: Box<dyn Cache<i32, String>> = create_cache(ReplacementPolicy::LRU, config);
        
        // Fill beyond capacity
        for i in 1..=10 {
            cache.put(i, format!("value_{}", i), 10);
        }

        // Should only keep 5 entries (50 bytes / 10 bytes per entry), rest evicted
        let stats = cache.stats();
        assert_eq!(stats.entries, 5);
        assert!(stats.evictions > 0);
    }

    #[test]
    fn test_multi_level_stats() {
        let l1_stats = CacheStats {
            entries: 10,
            hits: 80,
            misses: 20,
            evictions: 5,
            memory_usage_bytes: 1024 * 1024, // 1MB
            hit_ratio: 0.8,
        };

        let l2_stats = CacheStats {
            entries: 50,
            hits: 150,
            misses: 175,
            evictions: 20,
            memory_usage_bytes: 4 * 1024 * 1024, // 4MB
            hit_ratio: 0.3,
        };

        let l3_stats = CacheStats {
            entries: 200,
            hits: 100,
            misses: 300,
            evictions: 50,
            memory_usage_bytes: 10 * 1024 * 1024, // 10MB
            hit_ratio: 0.1,
        };

        let multi_stats = MultiLevelCacheStats::new(l1_stats, l2_stats, l3_stats);
        
        assert!((multi_stats.overall_hit_ratio - 0.4).abs() < 0.01); // ~40% overall
        assert_eq!(multi_stats.total_memory_usage, 15 * 1024 * 1024); // 15MB total
        
        let summary = multi_stats.summary();
        assert!(summary.contains("L1 (Hot): 80.00%"));
        assert!(summary.contains("L2 (Warm): 30.00%"));
        assert!(summary.contains("L3 (Cold): 10.00%"));
    }
}

impl<K: Hash + Eq + Clone + Send + Sync, V: Clone + Send + Sync> Cache<K, V> for Box<dyn Cache<K, V>> {
    fn get(&mut self, key: &K) -> Option<V> {
        (**self).get(key)
    }
    
    fn put(&mut self, key: K, value: V, size_bytes: usize) -> Option<()> {
        (**self).put(key, value, size_bytes)
    }
    
    fn remove(&mut self, key: &K) -> Option<V> {
        (**self).remove(key)
    }
    
    fn stats(&self) -> CacheStats {
        (**self).stats()
    }
    
    fn clear(&mut self) {
        (**self).clear()
    }
    
    fn size_bytes(&self) -> usize {
        (**self).size_bytes()
    }
}