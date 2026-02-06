//! Differential Dataflow Integration for Incremental Computation
//!
//! This module implements incremental computation using differential dataflow principles,
//! enabling real-time updates to materialized views with minimal recomputation.
//!
//! ## Core Concepts:
//! - Change Detection: Track data modifications at fine granularity
//! - Differential Updates: Compute only what changed, not full recomputation
//! - Propagation: Efficiently propagate changes through view dependencies
//! - Consistency: Maintain data consistency during incremental updates

use std::collections::{HashMap, HashSet};

use std::time::{Duration, Instant};

use graph_core::{VertexId, Edge, PropertyValue, Properties};
use super::{ViewType, ViewError, ViewCacheData};

/// Data change operation types
#[derive(Debug, Clone, PartialEq)]
pub enum DataChange {
    /// Vertex was added
    AddVertex {
        id: VertexId,
        properties: Properties,
    },
    /// Vertex was removed
    RemoveVertex {
        id: VertexId,
    properties: Properties, // Previous properties
    },
    /// Vertex properties were modified
    UpdateVertex {
        id: VertexId,
        old_properties: Properties,
        new_properties: Properties,
    },
    /// Edge was added
    AddEdge {
        edge: Edge,
        properties: Properties,
    },
    /// Edge was removed
    RemoveEdge {
        edge: Edge,
        properties: Properties, // Previous properties
    },
    /// Edge properties were modified
    UpdateEdge {
        edge: Edge,
        old_properties: Properties,
        new_properties: Properties,
    },
}

impl DataChange {
    /// Get timestamp for this change
    pub fn timestamp(&self) -> u64 {
        // In a real implementation, this would be from the system clock
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Get affected entities (vertex/edge IDs)
    pub fn affected_entities(&self) -> HashSet<String> {
        let mut entities = HashSet::new();
        
        match self {
            DataChange::AddVertex { id, .. } => {
                entities.insert(format!("vertex:{}", id));
            }
            DataChange::RemoveVertex { id, .. } => {
                entities.insert(format!("vertex:{}", id));
            }
            DataChange::UpdateVertex { id, .. } => {
                entities.insert(format!("vertex:{}", id));
            }
            DataChange::AddEdge { edge, .. } => {
                entities.insert(format!("edge:{}:{}", edge.src, edge.dst));
                entities.insert(format!("vertex:{}", edge.src));
                entities.insert(format!("vertex:{}", edge.dst));
            }
            DataChange::RemoveEdge { edge, .. } => {
                entities.insert(format!("edge:{}:{}", edge.src, edge.dst));
                entities.insert(format!("vertex:{}", edge.src));
                entities.insert(format!("vertex:{}", edge.dst));
            }
            DataChange::UpdateEdge { edge, .. } => {
                entities.insert(format!("edge:{}:{}", edge.src, edge.dst));
                entities.insert(format!("vertex:{}", edge.src));
                entities.insert(format!("vertex:{}", edge.dst));
            }
        }
        
        entities
    }

    /// Get change type name
    pub fn change_type(&self) -> &'static str {
        match self {
            DataChange::AddVertex { .. } => "add_vertex",
            DataChange::RemoveVertex { .. } => "remove_vertex",
            DataChange::UpdateVertex { .. } => "update_vertex",
            DataChange::AddEdge { .. } => "add_edge",
            DataChange::RemoveEdge { .. } => "remove_edge",
            DataChange::UpdateEdge { .. } => "update_edge",
        }
    }
}

/// Change collection with metadata
#[derive(Debug, Clone)]
pub struct ChangeSet {
    /// Unique identifier for this changeset
    pub id: String,
    /// Changes in this set
    pub changes: Vec<DataChange>,
    /// Timestamp when changeset was created
    pub timestamp: Instant,
    /// Source of changes (e.g., transaction_id, stream_name)
    pub source: String,
    /// Change batch metadata
    pub metadata: HashMap<String, PropertyValue>,
}

impl ChangeSet {
    /// Create new changeset
    pub fn new(id: String, source: String) -> Self {
        Self {
            id,
            changes: Vec::new(),
            timestamp: Instant::now(),
            source,
            metadata: HashMap::new(),
        }
    }

    /// Add a change to this set
    pub fn add_change(&mut self, change: DataChange) {
        self.changes.push(change);
    }

    /// Get number of changes
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// Check if changeset is empty
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Get all affected view types
    pub fn affected_view_types(&self) -> HashSet<ViewType> {
        let mut view_types = HashSet::new();
        
        for change in &self.changes {
            match change {
                DataChange::AddVertex { .. } |
                DataChange::UpdateVertex { .. } |
                DataChange::RemoveVertex { .. } => {
                    // Vertex operations affect lookup, analytics, and aggregation views
                    view_types.insert(ViewType::Lookup { 
                        key_vertices: Vec::new() 
                    });
                    view_types.insert(ViewType::Analytics { 
                        algorithm: "vertex_access".to_string() 
                    });
                    view_types.insert(ViewType::Aggregation { 
                        aggregate_type: "vertex_count".to_string() 
                    });
                }
                DataChange::AddEdge { .. } |
                DataChange::UpdateEdge { .. } |
                DataChange::RemoveEdge { .. } => {
                    // Edge operations affect traversal, connectivity, and path views
                    view_types.insert(ViewType::Analytics { 
                        algorithm: "graph_traversal".to_string() 
                    });
                    view_types.insert(ViewType::Analytics { 
                        algorithm: "connectivity".to_string() 
                    });
                }
            }
        }
        
        view_types
    }
}

/// Differential update result
#[derive(Debug, Clone)]
pub struct DifferentialUpdate {
    /// View being updated
    pub view_id: String,
    /// Previous cached data (if any)
    pub previous_data: Option<ViewCacheData>,
    /// New data after applying changes
    pub new_data: ViewCacheData,
    /// Time taken to compute the update
    pub computation_time_ms: u64,
    /// Number of changes applied
    pub changes_applied: usize,
    /// Memory usage difference
    pub memory_delta_bytes: i64,
}

impl DifferentialUpdate {
    /// Create new differential update
    pub fn new(
        view_id: String,
        previous_data: Option<ViewCacheData>,
        new_data: ViewCacheData,
        computation_time_ms: u64,
        changes_applied: usize,
    ) -> Self {
        let memory_delta = new_data.size_bytes() as i64 - 
            previous_data.as_ref().map(|d| d.size_bytes() as i64).unwrap_or(0);
            
        Self {
            view_id,
            previous_data,
            new_data,
            computation_time_ms,
            changes_applied,
            memory_delta_bytes: memory_delta,
        }
    }

    /// Check if update was successful
    pub fn is_success(&self) -> bool {
        self.computation_time_ms > 0 && self.changes_applied > 0
    }

    /// Get performance metrics
    pub fn performance_metrics(&self) -> HashMap<String, PropertyValue> {
        HashMap::from([
            ("computation_time_ms".to_string(), PropertyValue::float64(self.computation_time_ms as f64)),
            ("changes_applied".to_string(), PropertyValue::int64(self.changes_applied as i64)),
            ("memory_delta_bytes".to_string(), PropertyValue::int64(self.memory_delta_bytes)),
            ("new_data_size".to_string(), PropertyValue::int64(self.new_data.size_bytes() as i64)),
        ])
    }
}

/// Incremental computation configuration
#[derive(Debug, Clone)]
pub struct IncrementalConfig {
    /// Maximum time between change detections
    pub change_detection_interval: Duration,
    /// Batch size for processing changes
    pub batch_size: usize,
    /// Maximum number of pending changes before forced processing
    pub max_pending_changes: usize,
    /// Enable real-time propagation
    pub enable_realtime_propagation: bool,
    /// Maximum propagation delay
    pub max_propagation_delay: Duration,
}

impl Default for IncrementalConfig {
    fn default() -> Self {
        Self {
            change_detection_interval: Duration::from_millis(100), // 100ms
            batch_size: 1000,
            max_pending_changes: 10000,
            enable_realtime_propagation: true,
            max_propagation_delay: Duration::from_secs(5),
        }
    }
}

/// View dependency graph for incremental updates
#[derive(Debug, Clone)]
pub struct ViewDependency {
    /// Source view ID
    pub from_view: String,
    /// Target view ID
    pub to_view: String,
    /// Dependency type
    pub dependency_type: DependencyType,
    /// Strength of dependency (0.0-1.0)
    pub strength: f64,
}

/// Types of dependencies between views
#[derive(Debug, Clone, PartialEq)]
pub enum DependencyType {
    /// Data dependency - target depends on source's data
    Data,
    /// Schema dependency - target depends on source's structure
    Schema,
    /// Aggregation dependency - target aggregates from source
    Aggregation,
    /// Analytical dependency - target uses source for analytics
    Analytics,
}

/// Dependency graph manager
#[derive(Debug)]
pub struct DependencyGraph {
    /// Dependencies between views
    dependencies: HashMap<String, Vec<ViewDependency>>,
    /// Reverse dependencies (for invalidation propagation)
    reverse_dependencies: HashMap<String, Vec<String>>,
    /// Topological ordering cache
    topological_order: Vec<String>,
    /// Dirty views that need updates
    dirty_views: HashSet<String>,
}

impl DependencyGraph {
    /// Create new dependency graph
    pub fn new() -> Self {
        Self {
            dependencies: HashMap::new(),
            reverse_dependencies: HashMap::new(),
            topological_order: Vec::new(),
            dirty_views: HashSet::new(),
        }
    }

    /// Add dependency between views
    pub fn add_dependency(&mut self, from_view: String, to_view: String, dependency_type: DependencyType) {
        let dependency = ViewDependency {
            from_view: from_view.clone(),
            to_view: to_view.clone(),
            dependency_type,
            strength: 1.0,
        };

        self.dependencies
            .entry(from_view.clone())
            .or_insert_with(Vec::new)
            .push(dependency);

        // Add reverse dependency
        self.reverse_dependencies
            .entry(to_view.clone())
            .or_insert_with(Vec::new)
            .push(from_view.clone());

        // Invalidate topological order cache
        self.topological_order.clear();
    }

    /// Get all dependencies for a view
    pub fn get_dependencies(&self, view_id: &str) -> Vec<&ViewDependency> {
        self.dependencies
            .get(view_id)
            .map(|deps| deps.iter().collect())
            .unwrap_or_default()
    }

    /// Get all views that depend on this view
    pub fn get_dependents(&self, view_id: &str) -> Vec<&String> {
        self.reverse_dependencies
            .get(view_id)
            .map(|deps| deps.iter().collect())
            .unwrap_or_default()
    }

    /// Calculate topological order of views
    pub fn calculate_topological_order(&mut self) -> Result<(), ViewError> {
        let mut visited = HashSet::new();
        let mut order = Vec::new();
        let mut temp_mark = HashSet::new();

        for view_id in self.dependencies.keys() {
            if !visited.contains(view_id) {
                self.dfs_topological_sort(view_id, &mut visited, &mut temp_mark, &mut order)?;
            }
        }

        self.topological_order = order;
        Ok(())
    }

    /// Depth-first search for topological sort
    fn dfs_topological_sort(
        &self,
        view_id: &str,
        visited: &mut HashSet<String>,
        temp_mark: &mut HashSet<String>,
        order: &mut Vec<String>,
    ) -> Result<(), ViewError> {
        if temp_mark.contains(view_id) {
            return Err(ViewError::DependencyCycle {
                cycle: format!("Detected cycle involving view: {}", view_id),
            });
        }

        if !visited.contains(view_id) {
            temp_mark.insert(view_id.to_string());

            for dep in self.get_dependencies(view_id) {
                self.dfs_topological_sort(&dep.to_view, visited, temp_mark, order)?;
            }

            temp_mark.remove(view_id);
            visited.insert(view_id.to_string());
            order.push(view_id.to_string());
        }

        Ok(())
    }

    /// Mark view as dirty (needs update)
    pub fn mark_dirty(&mut self, view_id: &str) {
        self.dirty_views.insert(view_id.to_string());
    }

    /// Get views that need updates in topological order
    pub fn get_update_order(&mut self) -> Vec<String> {
        // Ensure topological order is up to date
        if self.topological_order.is_empty() {
            let _ = self.calculate_topological_order();
        }

        // Return dirty views in topological order
        self.topological_order
            .iter()
            .filter(|view_id| self.dirty_views.contains(*view_id))
            .cloned()
            .collect()
    }

    /// Mark view as clean (updated)
    pub fn mark_clean(&mut self, view_id: &str) {
        self.dirty_views.remove(view_id);
    }

    /// Check for dependency cycles
    pub fn detect_cycles(&self) -> Vec<String> {
        let mut cycles = Vec::new();
        let mut visited = HashSet::new();

        for view_id in self.dependencies.keys() {
            if !visited.contains(view_id) {
                if let Err(ViewError::DependencyCycle { cycle }) = 
                    self.dfs_detect_cycles(view_id, &mut visited, &mut HashSet::new()) {
                    cycles.push(cycle);
                }
            }
        }

        cycles
    }

    /// DFS for cycle detection
    fn dfs_detect_cycles(
        &self,
        view_id: &str,
        visited: &mut HashSet<String>,
        recursion_stack: &mut HashSet<String>,
    ) -> Result<(), ViewError> {
        visited.insert(view_id.to_string());
        recursion_stack.insert(view_id.to_string());

        for dep in self.get_dependencies(view_id) {
            if recursion_stack.contains(&dep.to_view) {
                return Err(ViewError::DependencyCycle {
                    cycle: format!("Cycle: {} -> {}", dep.from_view, dep.to_view),
                });
            }

            if !visited.contains(&dep.to_view) {
                self.dfs_detect_cycles(&dep.to_view, visited, recursion_stack)?;
            }
        }

        recursion_stack.remove(view_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_change_creation() {
        let change = DataChange::AddVertex {
            id: VertexId::new(1),
            properties: HashMap::new(),
        };

        assert_eq!(change.change_type(), "add_vertex");
        assert!(change.timestamp() > 0);
    }

    #[test]
    fn test_changeset_affected_views() {
        let mut changeset = ChangeSet::new("test".to_string(), "test_source".to_string());
        changeset.add_change(DataChange::AddVertex {
            id: VertexId::new(1),
            properties: HashMap::new(),
        });

        let affected = changeset.affected_view_types();
        assert!(!affected.is_empty());
    }

    #[test]
    fn test_dependency_graph() {
        let mut graph = DependencyGraph::new();
        graph.add_dependency("view1".to_string(), "view2".to_string(), DependencyType::Data);
        graph.add_dependency("view2".to_string(), "view3".to_string(), DependencyType::Aggregation);

        // view1 has 1 outgoing dependency (to view2)
        let deps = graph.get_dependencies("view1");
        assert_eq!(deps.len(), 1);

        // view2 has 1 outgoing dependency (to view3)
        let deps = graph.get_dependencies("view2");
        assert_eq!(deps.len(), 1);

        // view2 has 1 reverse dependency (from view1)
        let dependents = graph.get_dependents("view2");
        assert_eq!(dependents.len(), 1);
    }

    #[test]
    fn test_topological_sort() {
        let mut graph = DependencyGraph::new();
        graph.add_dependency("view1".to_string(), "view3".to_string(), DependencyType::Data);
        graph.add_dependency("view2".to_string(), "view3".to_string(), DependencyType::Data);
        graph.add_dependency("view3".to_string(), "view4".to_string(), DependencyType::Aggregation);

        assert!(graph.calculate_topological_order().is_ok());
        let order = graph.get_update_order();
        assert_eq!(order.len(), 0); // No dirty views
    }
}