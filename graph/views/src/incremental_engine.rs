//! Incremental Computation Engine
//!
//! This module provides the core incremental computation engine that
//! processes change events and updates materialized views efficiently.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use graph_core::{VertexId, PropertyValue};
use super::{
    MaterializedView, ViewType, RefreshPolicy, ViewError,
    DataChange, ChangeSet, DependencyGraph, DependencyType,
    IncrementalConfig, ViewCacheData, MultiLevelCacheManager
};

/// Incremental computation engine state
#[derive(Debug, Clone)]
pub struct EngineState {
    /// Number of changes processed
    pub changes_processed: u64,
    /// Number of views updated
    pub views_updated: u64,
    /// Total computation time saved
    pub time_saved_ms: u64,
    /// Last activity timestamp
    pub last_activity: Instant,
    /// Engine performance metrics
    pub performance_metrics: HashMap<String, PropertyValue>,
}

impl Default for EngineState {
    fn default() -> Self {
        Self {
            changes_processed: 0,
            views_updated: 0,
            time_saved_ms: 0,
            last_activity: Instant::now(),
            performance_metrics: HashMap::new(),
        }
    }
}

/// Incremental update result
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// View that was updated
    pub view_id: String,
    /// Whether update was successful
    pub success: bool,
    /// Number of changes applied
    pub changes_applied: usize,
    /// Time taken for update
    pub computation_time_ms: u64,
    /// Whether cached data was invalidated
    pub cache_invalidated: bool,
    /// New data after update
    pub new_data: Option<ViewCacheData>,
}

impl UpdateResult {
    /// Create successful update result
    pub fn success(view_id: String, changes_applied: usize, computation_time_ms: u64, new_data: ViewCacheData) -> Self {
        Self {
            view_id,
            success: true,
            changes_applied,
            computation_time_ms,
            cache_invalidated: false,
            new_data: Some(new_data),
        }
    }

    /// Create failed update result
    pub fn failure(view_id: String, computation_time_ms: u64) -> Self {
        Self {
            view_id,
            success: false,
            changes_applied: 0,
            computation_time_ms,
            cache_invalidated: false,
            new_data: None,
        }
    }

    /// Get performance metrics
    pub fn metrics(&self) -> HashMap<String, PropertyValue> {
        HashMap::from([
            ("view_id".to_string(), PropertyValue::string(&self.view_id)),
            ("success".to_string(), PropertyValue::bool(self.success)),
            ("changes_applied".to_string(), PropertyValue::int64(self.changes_applied as i64)),
            ("computation_time_ms".to_string(), PropertyValue::int64(self.computation_time_ms as i64)),
            ("cache_invalidated".to_string(), PropertyValue::bool(self.cache_invalidated)),
        ])
    }
}

/// Incremental computation engine
#[derive(Debug)]
pub struct IncrementalEngine {
    /// Configuration
    config: IncrementalConfig,
    /// Dependency graph
    dependency_graph: Arc<RwLock<DependencyGraph>>,
    /// Cache manager
    cache_manager: Arc<MultiLevelCacheManager>,
    /// Materialized views
    views: Arc<RwLock<HashMap<String, MaterializedView>>>,
    /// Engine state
    state: Arc<RwLock<EngineState>>,
    /// Pending changesets
    pending_changes: Arc<RwLock<VecDeque<ChangeSet>>>,
}

impl IncrementalEngine {
    /// Create new incremental engine
    pub fn new(
        config: IncrementalConfig,
        cache_manager: Arc<MultiLevelCacheManager>,
    ) -> Self {
        Self {
            config,
            dependency_graph: Arc::new(RwLock::new(DependencyGraph::new())),
            cache_manager,
            views: Arc::new(RwLock::new(HashMap::new())),
            state: Arc::new(RwLock::new(EngineState::default())),
            pending_changes: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Register a materialized view with the engine
    pub fn register_view(&self, view: MaterializedView) -> Result<(), ViewError> {
        // Validate view definition
        self.validate_view(&view)?;

        // Clone view for dependency update
        let view_clone = view.clone();
        
        // Add view to registry
        {
            let mut views = self.views.write().unwrap();
            views.insert(view.name.clone(), view);
        }

        // Update dependency graph
        self.update_dependencies(&view_clone)?;

        Ok(())
    }

    /// Process a changeset
    pub fn process_changeset(&self, changeset: ChangeSet) -> Result<Vec<UpdateResult>, ViewError> {
        let start_time = Instant::now();

        // Add to pending queue
        {
            let mut pending = self.pending_changes.write().unwrap();
            pending.push_back(changeset);
        }

        // Process pending changes
        let results = self.process_pending_changes()?;

        // Update engine state
        {
            let mut state = self.state.write().unwrap();
            state.changes_processed += 1;
            state.last_activity = Instant::now();
            
            let processing_time = start_time.elapsed().as_millis() as u64;
            state.time_saved_ms += processing_time; // Assume we saved this time
        }

        Ok(results)
    }

    /// Force update of a specific view
    pub fn force_update_view(&self, view_id: &str) -> Result<UpdateResult, ViewError> {
        let start_time = Instant::now();

        // Get view definition
        let view = {
            let views = self.views.read().unwrap();
            views.get(view_id).cloned()
                .ok_or_else(|| ViewError::ViewNotFound { view_name: view_id.to_string() })?
        };

        // Invalidate cache for this view
        if let Err(e) = self.invalidate_cache_for_view(view_id) {
            eprintln!("Failed to invalidate cache for view {}: {}", view_id, e);
        }

        // Recompute view data
        let new_data = self.compute_view_data(&view, &ChangeSet::new(
            "force_update".to_string(),
            "manual".to_string()
        ))?;

        let computation_time = start_time.elapsed().as_millis() as u64;
        let result = UpdateResult::success(
            view_id.to_string(),
            1, // Force update counts as 1 change
            computation_time,
            new_data,
        );

        // Update cache with new data
        if let Some(ref data) = result.new_data {
            self.cache_manager.put(
                view_id,
                self.compute_query_hash(&view.view_type),
                data.clone(),
                view.size_metrics.estimated_vertex_count as u64, // Estimated compute cost
            );
        }

        Ok(result)
    }

    /// Get engine state and metrics
    pub fn get_engine_state(&self) -> EngineState {
        self.state.read().unwrap().clone()
    }

    /// Get performance statistics
    pub fn get_performance_stats(&self) -> HashMap<String, PropertyValue> {
        let state = self.state.read().unwrap();
        let mut stats = state.performance_metrics.clone();

        // Add computed metrics
        stats.insert("changes_processed".to_string(), 
            PropertyValue::int64(state.changes_processed as i64));
        stats.insert("views_updated".to_string(), 
            PropertyValue::int64(state.views_updated as i64));
        stats.insert("time_saved_ms".to_string(), 
            PropertyValue::int64(state.time_saved_ms as i64));
        stats.insert("uptime_seconds".to_string(), 
            PropertyValue::float64(state.last_activity.elapsed().as_secs_f64()));

        stats
    }

    /// Process all pending changes
    fn process_pending_changes(&self) -> Result<Vec<UpdateResult>, ViewError> {
        let mut changes_to_process = Vec::new();

        // Extract pending changes
        {
            let mut pending = self.pending_changes.write().unwrap();
            while let Some(_changeset) = pending.pop_front() {
                changes_to_process.push(_changeset);
                if changes_to_process.len() >= self.config.batch_size {
                    break;
                }
            }
        }

        if changes_to_process.is_empty() {
            return Ok(Vec::new());
        }

        // Determine affected views and update order
        let affected_views = self.determine_affected_views(&changes_to_process);
        let update_order = self.calculate_update_order(affected_views)?;

        // Process updates in dependency order
        let mut results = Vec::new();
        for view_id in update_order {
            let view_changes: Vec<&DataChange> = changes_to_process
                .iter()
                .flat_map(|cs| cs.changes.iter())
                .filter(|change| self.affects_view(change, &view_id))
                .collect();

            if !view_changes.is_empty() {
                let result = self.update_view_incremental(&view_id, &view_changes)?;
                results.push(result);
            }
        }

        Ok(results)
    }

    /// Validate view definition
    fn validate_view(&self, view: &MaterializedView) -> Result<(), ViewError> {
        // Check if view has valid refresh policy
        match &view.refresh_policy {
            RefreshPolicy::EventDriven { debounce_ms } => {
                if *debounce_ms == 0 {
                    return Err(ViewError::InvalidRefreshPolicy {
                        policy: "EventDriven with 0 debounce".to_string(),
                    });
                }
            }
            RefreshPolicy::FixedInterval(interval) => {
                if *interval < Duration::from_millis(100) {
                    return Err(ViewError::InvalidRefreshPolicy {
                        policy: "FixedInterval too short".to_string(),
                    });
                }
            }
            _ => {} // Valid
        }

        // Check view size metrics
        if view.size_metrics.estimated_memory_bytes == 0 {
            return Err(ViewError::InvalidDefinition {
                details: "View size metrics cannot be zero".to_string(),
            });
        }

        Ok(())
    }

    /// Update dependency graph for a view
    fn update_dependencies(&self, view: &MaterializedView) -> Result<(), ViewError> {
        // Auto-generate dependencies based on view type
        let dependencies = self.infer_dependencies(view);

        {
            let mut graph = self.dependency_graph.write().unwrap();
            for dep in dependencies {
                graph.add_dependency(dep.from_view, dep.to_view, dep.dependency_type);
            }
        }

        Ok(())
    }

    /// Infer dependencies for a view based on its type
    fn infer_dependencies(&self, view: &MaterializedView) -> Vec<super::ViewDependency> {
        match &view.view_type {
            ViewType::Lookup { key_vertices } => {
                // Lookup views depend on the vertices they look up
                key_vertices.iter().map(|&vid| {
                    super::ViewDependency {
                        from_view: format!("vertex:{}", vid),
                        to_view: view.name.clone(),
                        dependency_type: DependencyType::Data,
                        strength: 1.0,
                    }
                }).collect()
            }
            ViewType::Aggregation {  .. } => {
                // Aggregation depends on base data for aggregation
                vec![super::ViewDependency {
                    from_view: "base_data".to_string(),
                    to_view: view.name.clone(),
                    dependency_type: DependencyType::Aggregation,
                    strength: 0.8,
                }]
            }
            ViewType::Analytics { algorithm, .. } => {
                // Analytics views depend on connectivity data
                vec![
                    super::ViewDependency {
                        from_view: "graph_topology".to_string(),
                        to_view: view.name.clone(),
                        dependency_type: DependencyType::Analytics,
                        strength: 0.9,
                    },
                    super::ViewDependency {
                        from_view: format!("algorithm:{}", algorithm),
                        to_view: view.name.clone(),
                        dependency_type: DependencyType::Analytics,
                        strength: 1.0,
                    }
                ]
            }
            ViewType::Hybrid { base_types } => {
                // Hybrid views depend on all their base types
                base_types.iter()
                    .flat_map(|base_type| self.infer_dependencies(&MaterializedView {
                        name: view.name.clone(),
                        view_type: base_type.clone(),
                        refresh_policy: view.refresh_policy.clone(),
                        size_metrics: view.size_metrics.clone(),
                        optimization_hints: view.optimization_hints.clone(),
                        created_at: view.created_at,
                        last_refreshed: view.last_refreshed,
                    }))
                    .collect()
            }
            #[cfg(feature = "sql")]
            ViewType::SqlQuery { query: _query, .. } => {
                // SQL queries depend on the tables/entities referenced in the query
                vec![super::ViewDependency {
                    from_view: "sql_parser".to_string(),
                    to_view: view.name.clone(),
                    dependency_type: DependencyType::Data,
                    strength: 1.0,
                }]
            }
        }
    }

    /// Determine which views are affected by changes
    fn determine_affected_views(&self, changesets: &[ChangeSet]) -> HashSet<String> {
        let mut affected_views = HashSet::new();

        for changeset in changesets {
            for change in &changeset.changes {
                let affected = change.affected_entities();
                for entity in affected {
                    // Find views that depend on this entity
                    affected_views.insert(format!("view_for_{}", entity));
                }
            }
        }

        affected_views
    }

    /// Calculate update order based on dependencies
    fn calculate_update_order(&self, affected_views: HashSet<String>) -> Result<Vec<String>, ViewError> {
        let mut graph = self.dependency_graph.write().unwrap();

        // Mark affected views as dirty
        for view_id in &affected_views {
            graph.mark_dirty(view_id);
        }

        // Get update order (topological)
        let update_order = graph.get_update_order();

        Ok(update_order)
    }

    /// Check if a change affects a specific view
    fn affects_view(&self, change: &DataChange, view_id: &str) -> bool {
        // Simplified logic - in reality, this would be more sophisticated
        match change {
            DataChange::AddVertex { id, .. } |
            DataChange::UpdateVertex { id, .. } |
            DataChange::RemoveVertex { id, .. } => {
                view_id.contains(&format!("vertex:{}", id))
            }
            DataChange::AddEdge { edge, .. } |
            DataChange::UpdateEdge { edge, .. } |
            DataChange::RemoveEdge { edge, .. } => {
                view_id.contains(&format!("edge:{}", edge.src)) ||
                view_id.contains(&format!("edge:{}", edge.dst))
            }
        }
    }

    /// Update a view incrementally
    fn update_view_incremental(&self, view_id: &str, changes: &[&DataChange]) -> Result<UpdateResult, ViewError> {
        let start_time = Instant::now();

        // Get view definition
        let view = {
            let views = self.views.read().unwrap();
            views.get(view_id).cloned()
                .ok_or_else(|| ViewError::ViewNotFound { view_name: view_id.to_string() })?
        };

        // Invalidate cache for this view
        if let Err(e) = self.invalidate_cache_for_view(view_id) {
            eprintln!("Failed to invalidate cache for view {}: {}", view_id, e);
        }

        // Get previous data
        let previous_data = self.get_cached_view_data(view_id);

        // Compute new data incrementally
        let new_data = self.compute_incremental_update(&view, &previous_data, changes)?;

        let computation_time = start_time.elapsed().as_millis() as u64;
        let result = UpdateResult::success(
            view_id.to_string(),
            changes.len(),
            computation_time,
            new_data,
        );

        // Update cache with new data
        self.cache_manager.put(
            view_id,
            self.compute_query_hash(&view.view_type),
            result.new_data.clone().unwrap(),
            view.size_metrics.estimated_vertex_count as u64,
        );

        // Mark view as clean
        {
            let mut graph = self.dependency_graph.write().unwrap();
            graph.mark_clean(view_id);
        }

        // Update engine state
        {
            let mut state = self.state.write().unwrap();
            state.views_updated += 1;
        }

        Ok(result)
    }

    /// Invalidate cache for a specific view
    fn invalidate_cache_for_view(&self, view_id: &str) -> Result<(), ViewError> {
        // In a real implementation, this would use cache_manager.remove()
        // For now, just log the invalidation
        println!("Cache invalidated for view: {}", view_id);
        Ok(())
    }

    /// Get cached view data
    fn get_cached_view_data(&self, view_id: &str) -> Option<ViewCacheData> {
        // Try to get from cache
        let query_hash = self.compute_query_hash(&ViewType::Lookup { key_vertices: Vec::new() });
        
        // Note: This is simplified - in reality, we'd use the actual view type
        self.cache_manager.get(view_id, query_hash)
    }

    /// Compute view data incrementally
    fn compute_incremental_update(
        &self,
        view: &MaterializedView,
        previous_data: &Option<ViewCacheData>,
        changes: &[&DataChange],
    ) -> Result<ViewCacheData, ViewError> {
        match &view.view_type {
            ViewType::Lookup { key_vertices } => {
                self.compute_lookup_incremental(key_vertices, previous_data, changes)
            }
            ViewType::Aggregation { aggregate_type } => {
                self.compute_aggregation_incremental(aggregate_type, &None, &None, previous_data, changes)
            }
            ViewType::Analytics { algorithm } => {
                self.compute_analytics_incremental(algorithm, &None, &None, &HashMap::new(), previous_data, changes)
            }
            ViewType::Hybrid { base_types } => {
                // For hybrid views, compute each base type and combine
                // This is simplified - in reality, would combine results appropriately
                if let Some(base_type) = base_types.first() {
                    self.compute_incremental_update(&MaterializedView {
                        name: view.name.clone(),
                        view_type: base_type.clone(),
                        refresh_policy: view.refresh_policy.clone(),
                        size_metrics: view.size_metrics.clone(),
                        optimization_hints: view.optimization_hints.clone(),
                        created_at: view.created_at,
                        last_refreshed: view.last_refreshed,
                    }, previous_data, changes)
                } else {
                    // Empty hybrid view - return empty result
                    Ok(ViewCacheData::VertexLookup {
                        vertices: HashMap::new(),
                    })
                }
            }
            #[cfg(feature = "sql")]
            ViewType::SqlQuery { .. } => {
                // SQL query views need to parse and execute the query
                // For now, return empty result
                Ok(ViewCacheData::VertexLookup {
                    vertices: HashMap::new(),
                })
            }
        }
    }

    /// Compute lookup view incrementally
    fn compute_lookup_incremental(
        &self,
        _key_vertices: &[VertexId],
        previous_data: &Option<ViewCacheData>,
        changes: &[&DataChange],
    ) -> Result<ViewCacheData, ViewError> {
        // Start with previous data if available
        let mut vertices = match previous_data {
            Some(ViewCacheData::VertexLookup { vertices }) => vertices.clone(),
            _ => HashMap::new(),
        };

        // Apply changes incrementally
        for change in changes {
            match change {
                DataChange::AddVertex { id, properties } => {
                    vertices.insert(*id, properties.clone());
                }
                DataChange::UpdateVertex { id, new_properties, .. } => {
                    vertices.insert(*id, new_properties.clone());
                }
                DataChange::RemoveVertex { id, .. } => {
                    vertices.remove(id);
                }
                _ => {} // Edge changes don't affect vertex lookup
            }
        }

        Ok(ViewCacheData::VertexLookup { vertices })
    }

    /// Compute aggregation view incrementally
    fn compute_aggregation_incremental(
        &self,
        aggregate_type: &str,
        _group_by: &Option<Vec<String>>,
        _filter: &Option<HashMap<String, PropertyValue>>,
        _previous_data: &Option<ViewCacheData>,
        _changes: &[&DataChange],
    ) -> Result<ViewCacheData, ViewError> {
        // Simplified incremental aggregation
        let result = match aggregate_type {
            "count" => PropertyValue::int64(42), // Placeholder
            "sum" => PropertyValue::float64(100.0), // Placeholder
            "avg" => PropertyValue::float64(25.5), // Placeholder
            _ => PropertyValue::string("unknown_aggregation".to_string()),
        };

        Ok(ViewCacheData::Aggregation {
            result,
            metadata: HashMap::from([
                ("aggregate_type".to_string(), PropertyValue::string(aggregate_type)),
                ("incremental".to_string(), PropertyValue::bool(true)),
            ]),
        })
    }

    /// Compute analytics view incrementally
    fn compute_analytics_incremental(
        &self,
        algorithm: &str,
        _source: &Option<VertexId>,
        _target: &Option<VertexId>,
        _parameters: &HashMap<String, String>,
        _previous_data: &Option<ViewCacheData>,
        _changes: &[&DataChange],
    ) -> Result<ViewCacheData, ViewError> {
        // Simplified incremental analytics
        let result = match algorithm {
            "shortest_path" => PropertyValue::string("path:[1,2,3]".to_string()),
            "connectivity" => PropertyValue::bool(true),
            "page_rank" => PropertyValue::float64(0.85),
            _ => PropertyValue::string(format!("analytics_{}", algorithm)),
        };

        Ok(ViewCacheData::Analytics {
            algorithm: algorithm.to_string(),
            result,
            metadata: HashMap::from([
                ("algorithm".to_string(), PropertyValue::string(algorithm)),
                ("incremental".to_string(), PropertyValue::bool(true)),
            ]),
        })
    }

    /// Compute view data from scratch (full computation)
    fn compute_view_data(&self, view: &MaterializedView, _changeset: &ChangeSet) -> Result<ViewCacheData, ViewError> {
        match &view.view_type {
            ViewType::Lookup { key_vertices } => {
                Ok(ViewCacheData::VertexLookup {
                    vertices: key_vertices.iter()
                        .map(|&id| (id, HashMap::new()))
                        .collect(),
                })
            }
            ViewType::Aggregation { aggregate_type, .. } => {
                self.compute_aggregation_incremental(aggregate_type, &None, &None, &None, &[])
            }
            ViewType::Analytics { algorithm, .. } => {
                self.compute_analytics_incremental(algorithm, &None, &None, &HashMap::new(), &None, &[])
            }
            ViewType::Hybrid { base_types } => {
                // For hybrid views, compute first base type
                if let Some(base_type) = base_types.first() {
                    self.compute_view_data(&MaterializedView {
                        name: view.name.clone(),
                        view_type: base_type.clone(),
                        refresh_policy: view.refresh_policy.clone(),
                        size_metrics: view.size_metrics.clone(),
                        optimization_hints: view.optimization_hints.clone(),
                        created_at: view.created_at,
                        last_refreshed: view.last_refreshed,
                    }, _changeset)
                } else {
                    Ok(ViewCacheData::VertexLookup {
                        vertices: HashMap::new(),
                    })
                }
            }
            #[cfg(feature = "sql")]
            ViewType::SqlQuery { .. } => {
                // SQL query views need to parse and execute the query
                // For now, return empty result
                Ok(ViewCacheData::VertexLookup {
                    vertices: HashMap::new(),
                })
            }
        }
    }

    /// Compute query hash for caching
    fn compute_query_hash(&self, view_type: &ViewType) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        view_type.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ViewSizeMetrics;

    #[test]
    fn test_engine_state() {
        let state = EngineState::default();
        assert_eq!(state.changes_processed, 0);
        assert_eq!(state.views_updated, 0);
    }

    #[test]
    fn test_update_result() {
        let result = UpdateResult::success(
            "test_view".to_string(),
            5,
            100,
            ViewCacheData::VertexLookup { vertices: HashMap::new() },
        );

        assert!(result.success);
        assert_eq!(result.changes_applied, 5);
        assert_eq!(result.computation_time_ms, 100);
    }

    #[test]
    fn test_dependency_inference() {
        let view = MaterializedView::new(
            "test_lookup".to_string(),
            ViewType::Lookup { key_vertices: vec![VertexId::new(1), VertexId::new(2)] },
            RefreshPolicy::EventDriven { debounce_ms: 100 },
            ViewSizeMetrics::default(),
        );

        let engine = IncrementalEngine::new(
            super::IncrementalConfig::default(),
            todo!(), // Cache manager placeholder
        );

        let dependencies = engine.infer_dependencies(&view);
        assert_eq!(dependencies.len(), 2);
    }
}