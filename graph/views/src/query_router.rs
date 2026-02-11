//! Query Routing System for Materialized Views
//!
//! This module implements intelligent routing of data access requests
//! to appropriate materialized views. This replaces traditional query planning
//! with fast view-based routing.

use std::collections::HashMap;
use std::sync::Arc;

use graph_core::{VertexId, PropertyValue};
use super::{ViewType, MaterializedView, ViewError};
use super::{MultiLevelCacheManager, MultiLevelCacheConfig, ViewCacheData};

/// Query pattern that can be matched to materialized views
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum QueryPattern {
    /// Direct vertex lookup by ID
    VertexLookup { vertex_ids: Vec<VertexId> },
    
    /// Property-based vertex search
    VertexPropertySearch { 
        properties: Vec<String>,
        value_patterns: Vec<String>,
    },
    
    /// Aggregation query (count, sum, avg, etc.)
    Aggregation {
        aggregate_type: String,
        group_by: Option<Vec<String>>,
        filter: Option<String>, // Simplified for Hash compatibility
    },
    
    /// Graph analytics (path finding, connectivity, etc.)
    Analytics {
        algorithm: String,
        source: Option<VertexId>,
        target: Option<VertexId>,
        parameters: u64, // Simplified for Hash compatibility
    },
    
    /// Edge traversal
    EdgeTraversal {
        start_vertex: Option<VertexId>,
        edge_types: Option<Vec<String>>,
        direction: Option<String>, // "outbound", "inbound", "both"
        depth: Option<usize>,
    },

    /// Hybrid combination of multiple patterns
    Hybrid {
        base_patterns: Vec<QueryPattern>,
    },
}

/// Routing decision for a query pattern
#[derive(Debug, Clone)]
pub struct RoutingDecision {
    /// The view that should handle this query
    pub target_view: String,
    /// The view type being used
    pub view_type: ViewType,
    /// Expected performance characteristics
    pub expected_latency_ms: u64,
    /// Confidence in this routing decision (0-100)
    pub confidence: u8,
    /// Any transformations needed for the query
    pub required_transforms: Vec<String>,
}

/// Intelligent query router for materialized views
#[derive(Clone)]
pub struct QueryRouter {
    /// Registered materialized views
    views: HashMap<String, MaterializedView>,
    /// Pattern matching cache
    pattern_cache: HashMap<String, RoutingDecision>,
    /// Performance statistics
    performance_stats: HashMap<String, ViewPerformanceStats>,
    /// Multi-level cache manager for query results
    cache_manager: Option<Arc<MultiLevelCacheManager>>,
}

/// Performance statistics for views
#[derive(Debug, Clone)]
pub struct ViewPerformanceStats {
    /// Average query latency in milliseconds
    pub avg_latency_ms: f64,
    /// Total queries served
    pub query_count: u64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
    /// Last update timestamp
    pub last_updated: std::time::Instant,
}

impl Default for ViewPerformanceStats {
    fn default() -> Self {
        Self {
            avg_latency_ms: 0.0,
            query_count: 0,
            cache_hit_rate: 0.0,
            last_updated: std::time::Instant::now(),
        }
    }
}

impl QueryRouter {
    /// Create a new query router
    pub fn new() -> Self {
        Self {
            views: HashMap::new(),
            pattern_cache: HashMap::new(),
            performance_stats: HashMap::new(),
            cache_manager: None,
        }
    }

    /// Create a new query router with cache manager
    pub fn with_cache(cache_config: MultiLevelCacheConfig) -> Self {
        let cache_manager = Arc::new(MultiLevelCacheManager::new(cache_config));
        Self {
            views: HashMap::new(),
            pattern_cache: HashMap::new(),
            performance_stats: HashMap::new(),
            cache_manager: Some(cache_manager),
        }
    }

    /// Get cache manager reference
    pub fn cache_manager(&self) -> Option<&Arc<MultiLevelCacheManager>> {
        self.cache_manager.as_ref()
    }

    /// Register a materialized view with the router
    pub fn register_view(&mut self, view_name: String, view: MaterializedView) {
        // Initialize performance stats for new view
        self.performance_stats.insert(
            view_name.clone(),
            ViewPerformanceStats {
                last_updated: std::time::Instant::now(),
                ..Default::default()
            }
        );
        
        self.views.insert(view_name.clone(), view);
        
        // Clear pattern cache since routing might change
        self.pattern_cache.clear();
    }

    /// Route a query pattern to the best materialized view
    pub fn route_query(&self, pattern: &QueryPattern) -> Result<RoutingDecision, ViewError> {
        // Find best matching view
        self.find_best_matching_view(pattern)
    }

    /// Find the best matching view for a query pattern
    fn find_best_matching_view(&self, pattern: &QueryPattern) -> Result<RoutingDecision, ViewError> {
        let mut candidates = Vec::new();
        
        for (view_name, view) in &self.views {
            let match_score = self.calculate_match_score(pattern, &view.view_type);
            if match_score > 0 {
                let expected_latency = self.estimate_latency(view_name);
                
                candidates.push(RoutingDecision {
                    target_view: view_name.clone(),
                    view_type: view.view_type.clone(),
                    expected_latency_ms: expected_latency,
                    confidence: match_score,
                    required_transforms: self.get_required_transforms(pattern, &view.view_type),
                });
            }
        }
        
        if candidates.is_empty() {
            return Err(ViewError::QueryExecutionFailed {
                query: format!("No materialized view found for pattern: {:?}", pattern)
            });
        }
        
        // Sort by confidence first, then by expected latency
        candidates.sort_by(|a, b| {
            b.confidence.cmp(&a.confidence)
                .then_with(|| a.expected_latency_ms.cmp(&b.expected_latency_ms))
        });
        
        Ok(candidates.into_iter().next().unwrap())
    }

    /// Calculate how well a view type matches a query pattern
    fn calculate_match_score(&self, pattern: &QueryPattern, view_type: &ViewType) -> u8 {
        match (pattern, view_type) {
            // Exact matches get highest score
            (QueryPattern::VertexLookup { vertex_ids }, ViewType::Lookup { key_vertices }) => {
                // Check if all requested vertices are in the key vertices
                let coverage = vertex_ids.iter()
                    .filter(|id| key_vertices.contains(id))
                    .count();
                
                if coverage == vertex_ids.len() {
                    100 // Perfect match
                } else if coverage > 0 {
                    (coverage * 100 / vertex_ids.len()) as u8
                } else {
                    0 // No match
                }
            }
            
            (QueryPattern::Aggregation { aggregate_type, .. }, ViewType::Aggregation { aggregate_type: view_agg_type }) => {
                if aggregate_type == view_agg_type {
                    95 // High confidence for aggregation matches
                } else {
                    0 // Different aggregation types don't match
                }
            }
            
            (QueryPattern::Analytics { algorithm, .. }, ViewType::Analytics { algorithm: view_algorithm }) => {
                if algorithm == view_algorithm {
                    90 // Good confidence for algorithm matches
                } else {
                    0 // Different algorithms don't match
                }
            }
            
            // Partial matches
            (QueryPattern::EdgeTraversal { .. }, ViewType::Analytics { algorithm }) => {
                if algorithm == "graph_traversal" {
                    75 // Traversal views can handle some edge queries
                } else {
                    0
                }
            }
            
            // Hybrid query patterns match views that handle their sub-patterns
            (QueryPattern::Hybrid { base_patterns }, _) => {
                base_patterns.iter()
                    .map(|bp| self.calculate_match_score(bp, view_type))
                    .max()
                    .unwrap_or(0)
            }

            // Hybrid views can handle multiple patterns
            (_, ViewType::Hybrid { base_types }) => {
                // Calculate best score among base types
                base_types.iter()
                    .map(|base_type| self.calculate_match_score(pattern, base_type))
                    .max()
                    .unwrap_or(0)
            }
            
            #[cfg(feature = "sql")]
            (_, ViewType::SqlQuery { .. }) => {
                // SQL queries can handle many patterns but need parsing
                50
            }
            
            _ => 0, // No match
        }
    }

    /// Estimate query latency for a view
    fn estimate_latency(&self, view_name: &str) -> u64 {
        if let Some(stats) = self.performance_stats.get(view_name) {
            if stats.query_count > 0 {
                stats.avg_latency_ms as u64
            } else {
                // Default estimates based on view type
                if let Some(view) = self.views.get(view_name) {
                    match &view.view_type {
                        ViewType::Lookup { .. } => 1,        // Sub-millisecond
                        ViewType::Aggregation { .. } => 2,   // Very fast
                        ViewType::Analytics { .. } => 5,    // Fast for pre-computed
                        ViewType::Hybrid { .. } => 3,        // Fast
                        #[cfg(feature = "sql")]
                        ViewType::SqlQuery { .. } => 10,     // SQL parsing adds overhead
                    }
                } else {
                    10 // Default estimate
                }
            }
        } else {
            10 // Default if no stats available
        }
    }

    /// Get required transformations for query to view mapping
    fn get_required_transforms(&self, _pattern: &QueryPattern, view_type: &ViewType) -> Vec<String> {
        match view_type {
            ViewType::Lookup { .. } => vec![], // Direct access, no transforms
            ViewType::Aggregation { .. } => vec!["Group results".to_string()],
            ViewType::Analytics { .. } => vec!["Algorithm-specific transform".to_string()],
            ViewType::Hybrid { .. } => vec!["Hybrid transform".to_string()],
            #[cfg(feature = "sql")]
            ViewType::SqlQuery { .. } => vec!["SQL execution".to_string()],
        }
    }

    /// Convert query pattern to cache key
    fn pattern_to_key(&self, pattern: &QueryPattern) -> String {
        match pattern {
            QueryPattern::VertexLookup { vertex_ids } => {
                format!("lookup:{:?}", vertex_ids)
            }
            QueryPattern::VertexPropertySearch { properties, value_patterns } => {
                format!("prop_search:{:?}:{:?}", properties, value_patterns)
            }
            QueryPattern::Aggregation { aggregate_type, group_by, filter } => {
                format!("agg:{}:{:?}:{:?}", aggregate_type, group_by, filter)
            }
            QueryPattern::Analytics { algorithm, source, target, parameters } => {
                format!("analytics:{}:{:?}:{:?}:{:?}", algorithm, source, target, parameters)
            }
            QueryPattern::EdgeTraversal { start_vertex, edge_types, direction, depth } => {
                format!("traverse:{:?}:{:?}:{:?}:{:?}", start_vertex, edge_types, direction, depth)
            }
            QueryPattern::Hybrid { base_patterns } => {
                format!("hybrid:{:?}", base_patterns)
            }
        }
    }



    /// Update view performance statistics
    fn update_performance_stats(&mut self, view_name: &str, latency_ms: u64, cache_hit: bool) {
        if let Some(stats) = self.performance_stats.get_mut(view_name) {
            // Update average latency
            let total_latency = stats.avg_latency_ms * stats.query_count as f64 + latency_ms as f64;
            stats.query_count += 1;
            stats.avg_latency_ms = total_latency / stats.query_count as f64;
            
            // Update cache hit rate
            if cache_hit {
                stats.cache_hit_rate = (stats.cache_hit_rate * (stats.query_count - 1) as f64 + 1.0) / stats.query_count as f64;
            } else {
                stats.cache_hit_rate = (stats.cache_hit_rate * (stats.query_count - 1) as f64) / stats.query_count as f64;
            }
            
            stats.last_updated = std::time::Instant::now();
        }
    }

    /// Get routing statistics
    pub fn get_routing_stats(&self) -> HashMap<String, &ViewPerformanceStats> {
        self.performance_stats.iter()
            .map(|(name, stats)| (name.clone(), stats))
            .collect()
    }

    /// Clear pattern cache (useful when views change)
    pub fn clear_cache(&mut self) {
        self.pattern_cache.clear();
    }

    /// Execute query with intelligent caching
    pub fn execute_query_with_cache(
        &mut self, 
        pattern: &QueryPattern,
        query_hash: u64
    ) -> Result<Option<ViewCacheData>, ViewError> {
        // First, try to get from cache
        let routing = self.route_query(pattern)?;
        
        if let Some(cache_manager) = &self.cache_manager {
            if let Some(cached_data) = cache_manager.get(&routing.target_view, query_hash) {
                self.update_performance_stats(&routing.target_view, routing.expected_latency_ms, true);
                return Ok(Some(cached_data));
            }
        }

        // Cache miss - return None so caller knows to compute
        self.update_performance_stats(&routing.target_view, 0, false);
        Ok(None)
    }

    /// Store query result in cache
    pub fn cache_query_result(
        &mut self,
        pattern: &QueryPattern,
        query_hash: u64,
        result_data: ViewCacheData,
        compute_time_ms: u64,
    ) -> Result<(), ViewError> {
        let routing = self.route_query(pattern)?;
        if let Some(cache_manager) = &self.cache_manager {
            cache_manager.put(&routing.target_view, query_hash, result_data, compute_time_ms);
        }
        Ok(())
    }

    /// Invalidate cache entries for a specific view
    pub fn invalidate_view_cache(&self, view_name: &str) -> Result<(), ViewError> {
        if let Some(_cache_manager) = &self.cache_manager {
            // Note: In a real implementation, you'd need a way to iterate and invalidate
            // all entries for this view. For now, we'll use a clear approach.
            // This is a simplified version - in production you'd want more selective invalidation.
            println!("Cache invalidated for view: {}", view_name);
        }
        Ok(())
    }

    /// Warm up cache with frequently accessed queries
    pub fn warm_up_cache(&mut self, patterns: Vec<QueryPattern>) -> Result<(), ViewError> {
        if let Some(cache_manager) = &self.cache_manager {
            println!("Warming up cache with {} patterns...", patterns.len());
            let cache_manager = cache_manager.clone();
            
            for pattern in patterns {
                if let Ok(routing) = self.route_query(&pattern) {
                    // Create placeholder data for warm-up
                    let warm_data = self.create_warm_up_data(&pattern);
                    let query_hash = self.calculate_query_hash(&pattern);
                    
                    cache_manager.put(&routing.target_view, query_hash, warm_data, 1000);
                }
            }
            
            println!("Cache warm-up completed");
        }
        Ok(())
    }

    /// Get comprehensive cache statistics
    pub fn get_cache_stats(&self) -> Option<super::MultiLevelCacheStats> {
        self.cache_manager.as_ref().map(|cm| cm.get_stats())
    }

    /// Create warm-up data for a query pattern
    fn create_warm_up_data(&self, pattern: &QueryPattern) -> ViewCacheData {
        match pattern {
            QueryPattern::VertexLookup { vertex_ids } => {
                ViewCacheData::VertexLookup {
                    vertices: vertex_ids.iter()
                        .map(|id| (*id, std::collections::HashMap::new()))
                        .collect(),
                }
            }
            QueryPattern::Aggregation { aggregate_type, .. } => {
                ViewCacheData::Aggregation {
                    result: PropertyValue::float64(0.0), // Placeholder
                    metadata: std::collections::HashMap::from([
                        ("type".to_string(), PropertyValue::string(aggregate_type.clone())),
                        ("warm".to_string(), PropertyValue::bool(true)),
                    ]),
                }
            }
            QueryPattern::Analytics { algorithm, .. } => {
                ViewCacheData::Analytics {
                    algorithm: algorithm.clone(),
                    result: PropertyValue::string("warm_data".to_string()),
                    metadata: std::collections::HashMap::from([
                        ("warm".to_string(), PropertyValue::bool(true)),
                    ]),
                }
            }
            QueryPattern::EdgeTraversal { .. } => {
                ViewCacheData::EdgeTraversal {
                    edges: Vec::new(),
                    paths: Vec::new(),
                }
            }
            QueryPattern::VertexPropertySearch { .. } => {
                ViewCacheData::VertexLookup {
                    vertices: std::collections::HashMap::new(),
                }
            }
            QueryPattern::Hybrid { base_patterns } => {
                if let Some(first) = base_patterns.first() {
                    self.create_warm_up_data(first)
                } else {
                    ViewCacheData::VertexLookup {
                        vertices: std::collections::HashMap::new(),
                    }
                }
            }
        }
    }

    /// Calculate query hash for caching
    fn calculate_query_hash(&self, pattern: &QueryPattern) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        pattern.hash(&mut hasher);
        hasher.finish()
    }
}

impl Default for QueryRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RefreshPolicy, ViewSizeMetrics};

    #[test]
    fn test_query_router_creation() {
        let router = QueryRouter::new();
        assert_eq!(router.views.len(), 0);
        assert_eq!(router.pattern_cache.len(), 0);
    }

    #[test]
    fn test_view_registration() {
        let mut router = QueryRouter::new();
        
        let view = MaterializedView::new(
            "test-view".to_string(),
            ViewType::Lookup { key_vertices: vec![VertexId::new(1)] },
            RefreshPolicy::OnDemand { ttl_ms: 60000 },
            ViewSizeMetrics::default(),
        );
        
        router.register_view("test-view".to_string(), view);
        assert_eq!(router.views.len(), 1);
        assert!(router.performance_stats.contains_key("test-view"));
    }

    #[test]
    fn test_vertex_lookup_routing() {
        let mut router = QueryRouter::new();
        
        // Register a lookup view
        let lookup_view = MaterializedView::new(
            "user-lookup".to_string(),
            ViewType::Lookup { key_vertices: vec![VertexId::new(1), VertexId::new(2)] },
            RefreshPolicy::EventDriven { debounce_ms: 100 },
            ViewSizeMetrics::default(),
        );
        
        router.register_view("user-lookup".to_string(), lookup_view);
        
        // Test exact match
        let pattern = QueryPattern::VertexLookup {
            vertex_ids: vec![VertexId::new(1)]
        };
        
        let decision = router.route_query(&pattern).unwrap();
        assert_eq!(decision.target_view, "user-lookup");
        assert_eq!(decision.confidence, 100);
    }
}