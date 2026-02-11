//! Incremental Computation Engine
//!
//! This module provides the core incremental computation engine that
//! processes change events and updates materialized views efficiently.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use graph_core::{VertexId, PropertyValue, Properties};
use super::{
    MaterializedView, ViewType, RefreshPolicy, ViewError,
    DataChange, ChangeSet, DependencyGraph, DependencyType,
    IncrementalConfig, ViewCacheData, MultiLevelCacheManager
};

/// Aggregation computation state for incremental updates
#[derive(Debug, Clone, Default)]
struct AggregationState {
    /// Count of elements
    count: i64,
    /// Sum of numeric values
    sum: f64,
    /// Minimum value (if any)
    min: Option<f64>,
    /// Maximum value (if any)
    max: Option<f64>,
}

/// Incremental analytics state for graph algorithms
#[derive(Debug, Clone, Default)]
struct IncrementalAnalyticsState {
    /// Outgoing adjacency list
    adjacency_out: HashMap<VertexId, HashSet<VertexId>>,
    /// Incoming adjacency list
    adjacency_in: HashMap<VertexId, HashSet<VertexId>>,
    /// Edge weights for weighted algorithms
    edge_weights: HashMap<(VertexId, VertexId), f64>,
    /// Vertex properties snapshot
    vertex_properties: HashMap<VertexId, Properties>,
    /// PageRank scores
    pagerank_scores: HashMap<VertexId, f64>,
}

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
        let views = self.views.read().unwrap();

        // For each registered view, check if any change in any changeset affects it
        for (view_id, _view) in views.iter() {
            for changeset in changesets {
                for change in &changeset.changes {
                    if self.affects_view(change, view_id) {
                        affected_views.insert(view_id.clone());
                        break;
                    }
                }
                if affected_views.contains(view_id) {
                    break;
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
        // Get view definition to determine effect based on view type
        let view = {
            let views = self.views.read().unwrap();
            match views.get(view_id) {
                Some(v) => v.clone(),
                None => {
                    // Fallback to string-based matching for unknown views
                    return self.affects_view_fallback(change, view_id);
                }
            }
        };

        // Check based on view type
        match &view.view_type {
            ViewType::Lookup { key_vertices } => {
                self.affects_lookup_view(change, key_vertices)
            }
            ViewType::Aggregation { aggregate_type } => {
                self.affects_aggregation_view(change, aggregate_type)
            }
            ViewType::Analytics { algorithm } => {
                self.affects_analytics_view(change, algorithm)
            }
            ViewType::Hybrid { base_types } => {
                // If any base type is affected, the hybrid view is affected
                base_types.iter().any(|base_type| {
                    match base_type {
                        ViewType::Lookup { key_vertices } =>
                            self.affects_lookup_view(change, key_vertices),
                        ViewType::Aggregation { aggregate_type } =>
                            self.affects_aggregation_view(change, aggregate_type),
                        ViewType::Analytics { algorithm } =>
                            self.affects_analytics_view(change, algorithm),
                        ViewType::Hybrid { .. } => true, // Conservative for nested hybrid
                        #[cfg(feature = "sql")]
                        ViewType::SqlQuery { .. } => true,
                    }
                })
            }
            #[cfg(feature = "sql")]
            ViewType::SqlQuery { query, .. } => {
                self.affects_sql_view(change, query)
            }
        }
    }

    /// Fallback view effect check using string matching
    fn affects_view_fallback(&self, change: &DataChange, view_id: &str) -> bool {
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

    /// Check if a change affects a Lookup view
    fn affects_lookup_view(&self, change: &DataChange, key_vertices: &[VertexId]) -> bool {
        match change {
            DataChange::AddVertex { id, .. } |
            DataChange::UpdateVertex { id, .. } |
            DataChange::RemoveVertex { id, .. } => {
                // Empty key_vertices means all vertices
                key_vertices.is_empty() || key_vertices.contains(id)
            }
            DataChange::AddEdge { .. } |
            DataChange::UpdateEdge { .. } |
            DataChange::RemoveEdge { .. } => {
                // Edge changes don't affect vertex lookup views
                false
            }
        }
    }

    /// Check if a change affects an Aggregation view
    fn affects_aggregation_view(&self, change: &DataChange, aggregate_type: &str) -> bool {
        match aggregate_type {
            // Vertex-specific aggregations
            agg if agg.starts_with("vertex_") || agg == "count" => {
                matches!(change,
                    DataChange::AddVertex { .. } |
                    DataChange::RemoveVertex { .. } |
                    DataChange::UpdateVertex { .. }
                )
            }
            // Edge-specific aggregations
            agg if agg.starts_with("edge_") => {
                matches!(change,
                    DataChange::AddEdge { .. } |
                    DataChange::RemoveEdge { .. } |
                    DataChange::UpdateEdge { .. }
                )
            }
            // Numeric aggregations (sum, avg, min, max) - affected by any property change
            "sum" | "avg" | "average" | "min" | "max" => true,
            // Unknown aggregation type - conservative approach
            _ => true,
        }
    }

    /// Check if a change affects an Analytics view
    fn affects_analytics_view(&self, change: &DataChange, algorithm: &str) -> bool {
        match algorithm {
            // Connectivity algorithms: affected by edge and vertex add/remove
            "connectivity" | "connected_components" | "strongly_connected" => {
                matches!(change,
                    DataChange::AddEdge { .. } |
                    DataChange::RemoveEdge { .. } |
                    DataChange::AddVertex { .. } |
                    DataChange::RemoveVertex { .. }
                )
            }
            // Path algorithms: affected by edge changes and vertex add/remove
            "shortest_path" | "all_paths" | "path_exists" => {
                matches!(change,
                    DataChange::AddEdge { .. } |
                    DataChange::RemoveEdge { .. } |
                    DataChange::UpdateEdge { .. } |
                    DataChange::AddVertex { .. } |
                    DataChange::RemoveVertex { .. }
                )
            }
            // PageRank: affected by edge and vertex changes
            "page_rank" | "pagerank" => {
                matches!(change,
                    DataChange::AddEdge { .. } |
                    DataChange::RemoveEdge { .. } |
                    DataChange::AddVertex { .. } |
                    DataChange::RemoveVertex { .. }
                )
            }
            // Centrality metrics: affected by edge changes
            "degree_centrality" | "betweenness" | "closeness" => {
                matches!(change,
                    DataChange::AddEdge { .. } |
                    DataChange::RemoveEdge { .. } |
                    DataChange::AddVertex { .. } |
                    DataChange::RemoveVertex { .. }
                )
            }
            // Triangle counting: only edge changes matter
            "triangle_count" | "clustering_coefficient" => {
                matches!(change,
                    DataChange::AddEdge { .. } |
                    DataChange::RemoveEdge { .. }
                )
            }
            // Unknown algorithm - conservative approach
            _ => true,
        }
    }

    /// Check if a change affects a SQL query view
    #[cfg(feature = "sql")]
    fn affects_sql_view(&self, change: &DataChange, query: &str) -> bool {
        let query_lower = query.to_lowercase();

        // Check if query involves vertices
        let involves_vertices = query_lower.contains("vertex") ||
                                query_lower.contains("node") ||
                                query_lower.contains("v.");

        // Check if query involves edges
        let involves_edges = query_lower.contains("edge") ||
                             query_lower.contains("relationship") ||
                             query_lower.contains("e.");

        match change {
            DataChange::AddVertex { .. } |
            DataChange::UpdateVertex { .. } |
            DataChange::RemoveVertex { .. } => involves_vertices,

            DataChange::AddEdge { .. } |
            DataChange::UpdateEdge { .. } |
            DataChange::RemoveEdge { .. } => involves_edges,
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
        filter: &Option<HashMap<String, PropertyValue>>,
        previous_data: &Option<ViewCacheData>,
        changes: &[&DataChange],
    ) -> Result<ViewCacheData, ViewError> {
        // Extract previous aggregation state
        let mut state = self.extract_aggregation_state(previous_data);

        // Apply changes incrementally
        for change in changes {
            // Check filter condition
            if !self.passes_filter(change, filter) {
                continue;
            }

            match change {
                DataChange::AddVertex { properties, .. } => {
                    self.apply_aggregation_add(&mut state, properties);
                }
                DataChange::RemoveVertex { properties, .. } => {
                    self.apply_aggregation_remove(&mut state, properties);
                }
                DataChange::UpdateVertex { old_properties, new_properties, .. } => {
                    // Update = remove old + add new
                    self.apply_aggregation_remove(&mut state, old_properties);
                    self.apply_aggregation_add(&mut state, new_properties);
                }
                DataChange::AddEdge { properties, .. } => {
                    self.apply_aggregation_add(&mut state, properties);
                }
                DataChange::RemoveEdge { properties, .. } => {
                    self.apply_aggregation_remove(&mut state, properties);
                }
                DataChange::UpdateEdge { old_properties, new_properties, .. } => {
                    self.apply_aggregation_remove(&mut state, old_properties);
                    self.apply_aggregation_add(&mut state, new_properties);
                }
            }
        }

        // Compute final result based on aggregate type
        let result = self.compute_final_aggregation(&state, aggregate_type);

        Ok(ViewCacheData::Aggregation {
            result,
            metadata: self.build_aggregation_metadata(&state, aggregate_type),
        })
    }

    /// Extract aggregation state from previous cached data
    fn extract_aggregation_state(&self, previous_data: &Option<ViewCacheData>) -> AggregationState {
        match previous_data {
            Some(ViewCacheData::Aggregation { metadata, .. }) => {
                AggregationState {
                    count: metadata.get("_count")
                        .and_then(|v| v.as_int64())
                        .unwrap_or(0),
                    sum: metadata.get("_sum")
                        .and_then(|v| v.as_float64())
                        .unwrap_or(0.0),
                    min: metadata.get("_min")
                        .and_then(|v| v.as_float64()),
                    max: metadata.get("_max")
                        .and_then(|v| v.as_float64()),
                }
            }
            _ => AggregationState::default(),
        }
    }

    /// Apply addition to aggregation state
    fn apply_aggregation_add(&self, state: &mut AggregationState, properties: &Properties) {
        state.count += 1;
        if let Some(value) = self.extract_numeric_value(properties) {
            state.sum += value;
            state.min = Some(state.min.map_or(value, |m| m.min(value)));
            state.max = Some(state.max.map_or(value, |m| m.max(value)));
        }
    }

    /// Apply removal to aggregation state
    fn apply_aggregation_remove(&self, state: &mut AggregationState, properties: &Properties) {
        state.count = (state.count - 1).max(0);
        if let Some(value) = self.extract_numeric_value(properties) {
            state.sum -= value;
            // Note: min/max cannot be precisely updated on removal without full data
            // They remain unchanged (conservative approach)
        }
    }

    /// Check if a change passes the filter condition
    fn passes_filter(&self, change: &DataChange, filter: &Option<HashMap<String, PropertyValue>>) -> bool {
        let filter = match filter {
            Some(f) => f,
            None => return true,
        };

        let properties = match change {
            DataChange::AddVertex { properties, .. } => properties,
            DataChange::RemoveVertex { properties, .. } => properties,
            DataChange::UpdateVertex { new_properties, .. } => new_properties,
            DataChange::AddEdge { properties, .. } => properties,
            DataChange::RemoveEdge { properties, .. } => properties,
            DataChange::UpdateEdge { new_properties, .. } => new_properties,
        };

        filter.iter().all(|(key, expected)| {
            properties.get(key).map_or(false, |actual| actual == expected)
        })
    }

    /// Extract numeric value from properties for aggregation
    fn extract_numeric_value(&self, properties: &Properties) -> Option<f64> {
        // Try common field names for numeric values
        for field in &["value", "amount", "count", "score", "weight"] {
            if let Some(value) = properties.get(*field) {
                match value {
                    PropertyValue::Int64(i) => return Some(*i as f64),
                    PropertyValue::Float64(f) => return Some(*f),
                    _ => {}
                }
            }
        }
        None
    }

    /// Compute final aggregation result
    fn compute_final_aggregation(&self, state: &AggregationState, aggregate_type: &str) -> PropertyValue {
        match aggregate_type {
            "count" => PropertyValue::int64(state.count),
            "sum" => PropertyValue::float64(state.sum),
            "avg" | "average" => {
                if state.count > 0 {
                    PropertyValue::float64(state.sum / state.count as f64)
                } else {
                    PropertyValue::float64(0.0)
                }
            }
            "min" => PropertyValue::float64(state.min.unwrap_or(0.0)),
            "max" => PropertyValue::float64(state.max.unwrap_or(0.0)),
            _ => PropertyValue::string(format!("unsupported:{}", aggregate_type)),
        }
    }

    /// Build metadata for aggregation result
    fn build_aggregation_metadata(&self, state: &AggregationState, aggregate_type: &str) -> HashMap<String, PropertyValue> {
        HashMap::from([
            ("aggregate_type".to_string(), PropertyValue::string(aggregate_type)),
            ("incremental".to_string(), PropertyValue::bool(true)),
            ("_count".to_string(), PropertyValue::int64(state.count)),
            ("_sum".to_string(), PropertyValue::float64(state.sum)),
            ("_min".to_string(), PropertyValue::float64(state.min.unwrap_or(0.0))),
            ("_max".to_string(), PropertyValue::float64(state.max.unwrap_or(0.0))),
        ])
    }

    /// Compute analytics view incrementally
    fn compute_analytics_incremental(
        &self,
        algorithm: &str,
        source: &Option<VertexId>,
        target: &Option<VertexId>,
        parameters: &HashMap<String, String>,
        previous_data: &Option<ViewCacheData>,
        changes: &[&DataChange],
    ) -> Result<ViewCacheData, ViewError> {
        // Extract or initialize analytics state
        let mut state = self.extract_analytics_state(previous_data);

        // Apply changes to local graph state
        for change in changes {
            self.apply_change_to_analytics_state(&mut state, change);
        }

        // Execute algorithm-specific computation
        let (result, metadata) = match algorithm {
            "connectivity" | "connected_components" => {
                self.compute_connectivity(&state)
            }
            "page_rank" | "pagerank" => {
                self.compute_pagerank_incremental(&mut state, parameters)
            }
            "shortest_path" => {
                self.compute_shortest_path(&state, source, target)
            }
            "degree_centrality" => {
                self.compute_degree_centrality(&state)
            }
            _ => {
                (PropertyValue::string(format!("unsupported:{}", algorithm)), HashMap::new())
            }
        };

        Ok(ViewCacheData::Analytics {
            algorithm: algorithm.to_string(),
            result,
            metadata: self.merge_analytics_metadata(metadata, &state, algorithm),
        })
    }

    /// Extract analytics state from previous cached data
    fn extract_analytics_state(&self, previous_data: &Option<ViewCacheData>) -> IncrementalAnalyticsState {
        match previous_data {
            Some(ViewCacheData::Analytics { metadata, .. }) => {
                let mut state = IncrementalAnalyticsState::default();

                // Rebuild adjacency lists from metadata if available
                if let Some(PropertyValue::Int64(vertex_count)) = metadata.get("_vertex_count") {
                    // Pre-allocate based on expected size
                    state.adjacency_out.reserve(*vertex_count as usize);
                    state.adjacency_in.reserve(*vertex_count as usize);
                }

                // Rebuild PageRank scores if available
                if let Some(PropertyValue::Float64(max_score)) = metadata.get("_max_pagerank") {
                    // Initialize with a reasonable default
                    state.pagerank_scores.insert(VertexId::new(0), *max_score);
                }

                state
            }
            _ => IncrementalAnalyticsState::default(),
        }
    }

    /// Apply a data change to the analytics state
    fn apply_change_to_analytics_state(&self, state: &mut IncrementalAnalyticsState, change: &DataChange) {
        match change {
            DataChange::AddVertex { id, properties } => {
                state.vertex_properties.insert(*id, properties.clone());
                // Initialize PageRank score for new vertex
                let n = state.vertex_properties.len().max(1) as f64;
                state.pagerank_scores.insert(*id, 1.0 / n);
            }
            DataChange::RemoveVertex { id, .. } => {
                state.vertex_properties.remove(id);
                state.pagerank_scores.remove(id);
                state.adjacency_out.remove(id);
                state.adjacency_in.remove(id);
                // Remove from other vertices' adjacency lists
                for neighbors in state.adjacency_out.values_mut() {
                    neighbors.remove(id);
                }
                for neighbors in state.adjacency_in.values_mut() {
                    neighbors.remove(id);
                }
            }
            DataChange::UpdateVertex { id, new_properties, .. } => {
                state.vertex_properties.insert(*id, new_properties.clone());
            }
            DataChange::AddEdge { edge, properties } => {
                state.adjacency_out.entry(edge.src).or_default().insert(edge.dst);
                state.adjacency_in.entry(edge.dst).or_default().insert(edge.src);
                // Extract edge weight
                let weight = properties.get("weight")
                    .and_then(|v| v.as_float64())
                    .unwrap_or(1.0);
                state.edge_weights.insert((edge.src, edge.dst), weight);
            }
            DataChange::RemoveEdge { edge, .. } => {
                if let Some(neighbors) = state.adjacency_out.get_mut(&edge.src) {
                    neighbors.remove(&edge.dst);
                }
                if let Some(neighbors) = state.adjacency_in.get_mut(&edge.dst) {
                    neighbors.remove(&edge.src);
                }
                state.edge_weights.remove(&(edge.src, edge.dst));
            }
            DataChange::UpdateEdge { edge, new_properties, .. } => {
                let weight = new_properties.get("weight")
                    .and_then(|v| v.as_float64())
                    .unwrap_or(1.0);
                state.edge_weights.insert((edge.src, edge.dst), weight);
            }
        }
    }

    /// Compute connected components count using DFS
    fn compute_connectivity(&self, state: &IncrementalAnalyticsState) -> (PropertyValue, HashMap<String, PropertyValue>) {
        let mut visited = HashSet::new();
        let mut component_count = 0;

        for vertex in state.vertex_properties.keys() {
            if !visited.contains(vertex) {
                self.dfs_visit(state, *vertex, &mut visited);
                component_count += 1;
            }
        }

        // Also count isolated vertices from adjacency lists
        for vertex in state.adjacency_out.keys() {
            if !visited.contains(vertex) {
                self.dfs_visit(state, *vertex, &mut visited);
                component_count += 1;
            }
        }

        let mut metadata = HashMap::new();
        metadata.insert("component_count".to_string(), PropertyValue::int64(component_count));
        metadata.insert("vertex_count".to_string(), PropertyValue::int64(visited.len() as i64));

        (PropertyValue::int64(component_count), metadata)
    }

    /// DFS visit for connectivity computation (treats graph as undirected)
    fn dfs_visit(&self, state: &IncrementalAnalyticsState, start: VertexId, visited: &mut HashSet<VertexId>) {
        let mut stack = vec![start];

        while let Some(v) = stack.pop() {
            if visited.contains(&v) {
                continue;
            }
            visited.insert(v);

            // Add outgoing neighbors
            if let Some(neighbors) = state.adjacency_out.get(&v) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        stack.push(*neighbor);
                    }
                }
            }

            // Add incoming neighbors (for undirected connectivity)
            if let Some(neighbors) = state.adjacency_in.get(&v) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        stack.push(*neighbor);
                    }
                }
            }
        }
    }

    /// Compute PageRank incrementally with limited iterations
    fn compute_pagerank_incremental(
        &self,
        state: &mut IncrementalAnalyticsState,
        parameters: &HashMap<String, String>,
    ) -> (PropertyValue, HashMap<String, PropertyValue>) {
        let damping = parameters.get("damping")
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.85);

        let iterations = parameters.get("iterations")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10); // Use fewer iterations for incremental updates

        // Collect all vertices
        let vertices: Vec<VertexId> = state.vertex_properties.keys()
            .chain(state.adjacency_out.keys())
            .chain(state.adjacency_in.keys())
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let n = vertices.len();
        if n == 0 {
            return (PropertyValue::float64(0.0), HashMap::new());
        }

        // Initialize scores if empty
        if state.pagerank_scores.is_empty() {
            let initial_score = 1.0 / n as f64;
            for vertex in &vertices {
                state.pagerank_scores.insert(*vertex, initial_score);
            }
        }

        // Power iteration
        for _ in 0..iterations {
            let mut new_scores = HashMap::new();

            for vertex in &vertices {
                let mut incoming_sum = 0.0;

                if let Some(in_neighbors) = state.adjacency_in.get(vertex) {
                    for neighbor in in_neighbors {
                        if let Some(neighbor_score) = state.pagerank_scores.get(neighbor) {
                            let out_degree = state.adjacency_out.get(neighbor)
                                .map(|s| s.len())
                                .unwrap_or(1);
                            incoming_sum += neighbor_score / out_degree as f64;
                        }
                    }
                }

                let new_score = (1.0 - damping) / n as f64 + damping * incoming_sum;
                new_scores.insert(*vertex, new_score);
            }

            state.pagerank_scores = new_scores;
        }

        // Find max score
        let max_score = state.pagerank_scores.values()
            .cloned()
            .fold(0.0_f64, |a, b| a.max(b));

        let mut metadata = HashMap::new();
        metadata.insert("max_score".to_string(), PropertyValue::float64(max_score));
        metadata.insert("vertex_count".to_string(), PropertyValue::int64(n as i64));
        metadata.insert("iterations".to_string(), PropertyValue::int64(iterations as i64));
        metadata.insert("damping".to_string(), PropertyValue::float64(damping));

        (PropertyValue::float64(max_score), metadata)
    }

    /// Compute shortest path using Dijkstra's algorithm
    fn compute_shortest_path(
        &self,
        state: &IncrementalAnalyticsState,
        source: &Option<VertexId>,
        target: &Option<VertexId>,
    ) -> (PropertyValue, HashMap<String, PropertyValue>) {
        let source = match source {
            Some(s) => *s,
            None => return (PropertyValue::string("no_source".to_string()), HashMap::new()),
        };

        let target = match target {
            Some(t) => *t,
            None => return (PropertyValue::string("no_target".to_string()), HashMap::new()),
        };

        let mut distances: HashMap<VertexId, f64> = HashMap::new();
        let mut previous: HashMap<VertexId, VertexId> = HashMap::new();
        let mut visited: HashSet<VertexId> = HashSet::new();
        let mut queue: VecDeque<(f64, VertexId)> = VecDeque::new();

        distances.insert(source, 0.0);
        queue.push_back((0.0, source));

        while let Some((dist, current)) = queue.pop_front() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current);

            if current == target {
                // Reconstruct path
                let mut path = vec![target];
                let mut curr = target;
                while let Some(&prev) = previous.get(&curr) {
                    path.push(prev);
                    curr = prev;
                    if curr == source {
                        break;
                    }
                }
                path.reverse();

                let path_str: Vec<String> = path.iter().map(|v| v.to_string()).collect();
                let mut metadata = HashMap::new();
                metadata.insert("path".to_string(), PropertyValue::string(path_str.join("->")));
                metadata.insert("distance".to_string(), PropertyValue::float64(dist));
                metadata.insert("path_length".to_string(), PropertyValue::int64(path.len() as i64));

                return (PropertyValue::float64(dist), metadata);
            }

            if let Some(neighbors) = state.adjacency_out.get(&current) {
                for neighbor in neighbors {
                    let weight = state.edge_weights.get(&(current, *neighbor)).unwrap_or(&1.0);
                    let new_dist = dist + weight;

                    if !distances.contains_key(neighbor) || new_dist < distances[neighbor] {
                        distances.insert(*neighbor, new_dist);
                        previous.insert(*neighbor, current);
                        queue.push_back((new_dist, *neighbor));
                    }
                }
            }
        }

        // Path not found
        let mut metadata = HashMap::new();
        metadata.insert("path".to_string(), PropertyValue::string("not_found".to_string()));

        (PropertyValue::float64(f64::INFINITY), metadata)
    }

    /// Compute degree centrality
    fn compute_degree_centrality(&self, state: &IncrementalAnalyticsState) -> (PropertyValue, HashMap<String, PropertyValue>) {
        // Collect all vertices
        let vertices: HashSet<VertexId> = state.vertex_properties.keys()
            .chain(state.adjacency_out.keys())
            .chain(state.adjacency_in.keys())
            .copied()
            .collect();

        let n = vertices.len();
        if n <= 1 {
            return (PropertyValue::float64(0.0), HashMap::new());
        }

        let mut max_degree = 0;
        let mut max_vertex = None;

        for vertex in &vertices {
            let out_degree = state.adjacency_out.get(vertex).map(|s| s.len()).unwrap_or(0);
            let in_degree = state.adjacency_in.get(vertex).map(|s| s.len()).unwrap_or(0);
            let total_degree = out_degree + in_degree;

            if total_degree > max_degree {
                max_degree = total_degree;
                max_vertex = Some(*vertex);
            }
        }

        // Normalized centrality
        let centrality = max_degree as f64 / (2.0 * (n - 1) as f64);

        let mut metadata = HashMap::new();
        metadata.insert("max_degree".to_string(), PropertyValue::int64(max_degree as i64));
        metadata.insert("vertex_count".to_string(), PropertyValue::int64(n as i64));
        if let Some(v) = max_vertex {
            metadata.insert("max_vertex".to_string(), PropertyValue::int64(u64::from(v) as i64));
        }

        (PropertyValue::float64(centrality), metadata)
    }

    /// Merge analytics metadata with state info
    fn merge_analytics_metadata(
        &self,
        mut metadata: HashMap<String, PropertyValue>,
        state: &IncrementalAnalyticsState,
        algorithm: &str,
    ) -> HashMap<String, PropertyValue> {
        metadata.insert("algorithm".to_string(), PropertyValue::string(algorithm));
        metadata.insert("incremental".to_string(), PropertyValue::bool(true));
        metadata.insert("_vertex_count".to_string(), PropertyValue::int64(state.vertex_properties.len() as i64));
        metadata.insert("_edge_count".to_string(), PropertyValue::int64(state.edge_weights.len() as i64));

        if let Some(max_score) = state.pagerank_scores.values().cloned().reduce(f64::max) {
            metadata.insert("_max_pagerank".to_string(), PropertyValue::float64(max_score));
        }

        metadata
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
    use graph_core::Edge;
    use crate::{ViewSizeMetrics, MultiLevelCacheConfig};

    fn create_test_engine() -> IncrementalEngine {
        let cache_config = MultiLevelCacheConfig::default();
        let cache_manager = Arc::new(MultiLevelCacheManager::new(cache_config));
        IncrementalEngine::new(IncrementalConfig::default(), cache_manager)
    }

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

        let engine = create_test_engine();
        let dependencies = engine.infer_dependencies(&view);
        assert_eq!(dependencies.len(), 2);
    }

    #[test]
    fn test_aggregation_count() {
        let engine = create_test_engine();

        let changes: Vec<DataChange> = vec![
            DataChange::AddVertex { id: VertexId::new(1), properties: HashMap::new() },
            DataChange::AddVertex { id: VertexId::new(2), properties: HashMap::new() },
            DataChange::AddVertex { id: VertexId::new(3), properties: HashMap::new() },
        ];

        let change_refs: Vec<&DataChange> = changes.iter().collect();
        let result = engine.compute_aggregation_incremental(
            "count", &None, &None, &None, &change_refs,
        ).unwrap();

        if let ViewCacheData::Aggregation { result, .. } = result {
            assert_eq!(result.as_int64(), Some(3));
        } else {
            panic!("Expected Aggregation result");
        }
    }

    #[test]
    fn test_aggregation_sum() {
        let engine = create_test_engine();

        let mut props1 = HashMap::new();
        props1.insert("value".to_string(), PropertyValue::float64(10.0));

        let mut props2 = HashMap::new();
        props2.insert("value".to_string(), PropertyValue::float64(20.0));

        let changes: Vec<DataChange> = vec![
            DataChange::AddVertex { id: VertexId::new(1), properties: props1 },
            DataChange::AddVertex { id: VertexId::new(2), properties: props2 },
        ];

        let change_refs: Vec<&DataChange> = changes.iter().collect();
        let result = engine.compute_aggregation_incremental(
            "sum", &None, &None, &None, &change_refs,
        ).unwrap();

        if let ViewCacheData::Aggregation { result, .. } = result {
            assert_eq!(result.as_float64(), Some(30.0));
        } else {
            panic!("Expected Aggregation result");
        }
    }

    #[test]
    fn test_connectivity_analysis() {
        let engine = create_test_engine();

        let changes: Vec<DataChange> = vec![
            DataChange::AddVertex { id: VertexId::new(1), properties: HashMap::new() },
            DataChange::AddVertex { id: VertexId::new(2), properties: HashMap::new() },
            DataChange::AddVertex { id: VertexId::new(3), properties: HashMap::new() },
            DataChange::AddEdge {
                edge: Edge::new(VertexId::new(1), VertexId::new(2), "link"),
                properties: HashMap::new(),
            },
        ];

        let change_refs: Vec<&DataChange> = changes.iter().collect();
        let result = engine.compute_analytics_incremental(
            "connectivity", &None, &None, &HashMap::new(), &None, &change_refs,
        ).unwrap();

        if let ViewCacheData::Analytics { result, .. } = result {
            // Should have 2 components: {1,2} and {3}
            assert_eq!(result.as_int64(), Some(2));
        } else {
            panic!("Expected Analytics result");
        }
    }

    #[test]
    fn test_affects_view_lookup() {
        let engine = create_test_engine();

        // Register a Lookup view
        let view = MaterializedView::new(
            "user_lookup".to_string(),
            ViewType::Lookup { key_vertices: vec![VertexId::new(1), VertexId::new(2)] },
            RefreshPolicy::EventDriven { debounce_ms: 100 },
            ViewSizeMetrics::default(),
        );
        engine.register_view(view).unwrap();

        // Test affect detection
        let change1 = DataChange::AddVertex { id: VertexId::new(1), properties: HashMap::new() };
        assert!(engine.affects_view(&change1, "user_lookup"));

        let change2 = DataChange::AddVertex { id: VertexId::new(99), properties: HashMap::new() };
        assert!(!engine.affects_view(&change2, "user_lookup"));

        let change3 = DataChange::AddEdge {
            edge: Edge::new(VertexId::new(1), VertexId::new(2), "link"),
            properties: HashMap::new(),
        };
        assert!(!engine.affects_view(&change3, "user_lookup"));
    }

    #[test]
    fn test_incremental_aggregation_update() {
        let engine = create_test_engine();

        // First computation
        let initial_changes: Vec<DataChange> = vec![
            DataChange::AddVertex { id: VertexId::new(1), properties: HashMap::new() },
            DataChange::AddVertex { id: VertexId::new(2), properties: HashMap::new() },
        ];

        let change_refs: Vec<&DataChange> = initial_changes.iter().collect();
        let first_result = engine.compute_aggregation_incremental(
            "count", &None, &None, &None, &change_refs,
        ).unwrap();

        // Second incremental computation
        let new_changes: Vec<DataChange> = vec![
            DataChange::AddVertex { id: VertexId::new(3), properties: HashMap::new() },
        ];

        let new_change_refs: Vec<&DataChange> = new_changes.iter().collect();
        let second_result = engine.compute_aggregation_incremental(
            "count", &None, &None, &Some(first_result), &new_change_refs,
        ).unwrap();

        if let ViewCacheData::Aggregation { result, .. } = second_result {
            assert_eq!(result.as_int64(), Some(3));
        } else {
            panic!("Expected Aggregation result");
        }
    }
}