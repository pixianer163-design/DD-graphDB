//! Simplified Storage Integration for Materialized Views
//!
//! This is a simplified version that focuses on core functionality without
//! the complexity of the full implementation.

use std::collections::HashMap;
use std::sync::Arc;

use graph_core::{VertexId, Edge, Properties};
use graph_storage::{GraphStorage, DatabaseStats};

use crate::view_types::{ViewType};
use crate::{ViewError, DataChange, ChangeSet, IncrementalEngine};

/// Simple data change event
#[derive(Debug, Clone)]
pub enum DataChangeEvent {
    VertexChanged { id: VertexId },
    VertexRemoved { id: VertexId },
    EdgeChanged { edge: Edge },
    EdgeRemoved { edge: Edge },
}

impl DataChangeEvent {
    /// Get the affected vertex IDs for this event
    pub fn affected_vertices(&self) -> Vec<VertexId> {
        match self {
            DataChangeEvent::VertexChanged { id } => vec![*id],
            DataChangeEvent::VertexRemoved { id } => vec![*id],
            DataChangeEvent::EdgeChanged { edge } => vec![edge.src, edge.dst],
            DataChangeEvent::EdgeRemoved { edge } => vec![edge.src, edge.dst],
        }
    }
}

/// Type alias for backward compatibility
pub type MaterializedViewError = ViewError;

/// Read-only interface for views to access graph data
pub trait ViewDataReader {
    fn get_vertex(&self, id: VertexId) -> Result<Option<Properties>, MaterializedViewError>;
    fn get_database_stats(&self) -> Result<DatabaseStats, MaterializedViewError>;
}

/// Simple storage integration
#[derive(Debug)]
pub struct StorageIntegration {
    storage: Arc<GraphStorage>,
}

impl StorageIntegration {
    /// Create a new storage integration
    pub fn new(storage: Arc<GraphStorage>) -> Self {
        Self { storage }
    }

    /// Get direct access to the underlying storage
    pub fn storage(&self) -> &GraphStorage {
        &self.storage
    }

    /// Commit a transaction with change notification (simplified)
    pub fn commit_with_events(&self, transaction: graph_storage::Transaction) -> Result<(), MaterializedViewError> {
        self.storage.commit_transaction(transaction)
            .map_err(|e| MaterializedViewError::StorageError { error: e.to_string() })
    }
}

impl ViewDataReader for StorageIntegration {
    fn get_vertex(&self, id: VertexId) -> Result<Option<Properties>, MaterializedViewError> {
        self.storage.get_vertex(id)
            .map_err(|e| MaterializedViewError::StorageError { error: e.to_string() })
    }

    fn get_database_stats(&self) -> Result<DatabaseStats, MaterializedViewError> {
        self.storage.get_stats()
            .map_err(|e| MaterializedViewError::StorageError { error: e.to_string() })
    }
}

/// Simple change listener
pub trait DataChangeListener: Send + Sync {
    fn on_data_change(&self, event: &DataChangeEvent) -> Result<(), MaterializedViewError>;
    fn view_id(&self) -> &str;
}

/// Simple view refresh listener
#[derive(Debug)]
pub struct ViewRefreshListener {
    view_id: String,
}

impl ViewRefreshListener {
    pub fn new(view_id: String) -> Self {
        Self { view_id }
    }
}

impl DataChangeListener for ViewRefreshListener {
    fn on_data_change(&self, _event: &DataChangeEvent) -> Result<(), MaterializedViewError> {
        // Simple implementation - just return success
        println!("View {} notified of data change", self.view_id);
        Ok(())
    }

    fn view_id(&self) -> &str {
        &self.view_id
    }
}

/// Simple integrated view manager
pub struct IntegratedViewManager {
    storage_integration: Arc<StorageIntegration>,
    listeners: std::sync::RwLock<HashMap<String, Box<dyn DataChangeListener>>>,
    incremental_engine: Option<Arc<IncrementalEngine>>,
}

impl std::fmt::Debug for IntegratedViewManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let listener_count = self.listeners.read()
            .map(|l| l.len())
            .unwrap_or(0);
        f.debug_struct("IntegratedViewManager")
            .field("listener_count", &listener_count)
            .field("has_engine", &self.incremental_engine.is_some())
            .finish()
    }
}

impl IntegratedViewManager {
    /// Create a new integrated view manager
    pub fn new(storage_integration: Arc<StorageIntegration>) -> Self {
        Self {
            storage_integration,
            listeners: std::sync::RwLock::new(HashMap::new()),
            incremental_engine: None,
        }
    }

    /// Create with an incremental engine for end-to-end change propagation
    pub fn with_engine(storage_integration: Arc<StorageIntegration>, engine: Arc<IncrementalEngine>) -> Self {
        Self {
            storage_integration,
            listeners: std::sync::RwLock::new(HashMap::new()),
            incremental_engine: Some(engine),
        }
    }

    /// Get the storage integration
    pub fn storage_integration(&self) -> &Arc<StorageIntegration> {
        &self.storage_integration
    }

    /// Get the incremental engine (if configured)
    pub fn engine(&self) -> Option<&Arc<IncrementalEngine>> {
        self.incremental_engine.as_ref()
    }

    /// Register a view with a change listener
    pub fn register_view(&self, view_id: String, _view_type: ViewType) -> Result<(), MaterializedViewError> {
        let listener = ViewRefreshListener::new(view_id.clone());
        let mut listeners = self.listeners.write()
            .map_err(|e| MaterializedViewError::StorageError { error: e.to_string() })?;
        listeners.insert(view_id, Box::new(listener));
        Ok(())
    }

    /// Notify all registered listeners of a data change, and propagate
    /// through the incremental engine if one is attached.
    pub fn notify_change(&self, event: &DataChangeEvent) -> Result<(), MaterializedViewError> {
        // Notify simple listeners
        let listeners = self.listeners.read()
            .map_err(|e| MaterializedViewError::StorageError { error: e.to_string() })?;
        for listener in listeners.values() {
            listener.on_data_change(event)?;
        }
        drop(listeners);

        // Propagate to incremental engine
        if let Some(engine) = &self.incremental_engine {
            if let Some(data_change) = Self::event_to_data_change(event) {
                let mut changeset = ChangeSet::new(
                    format!("storage_event_{}", std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()),
                    "storage_integration".to_string(),
                );
                changeset.add_change(data_change);
                engine.process_changeset(changeset)
                    .map_err(|e| MaterializedViewError::StorageError { error: e.to_string() })?;
            }
        }

        Ok(())
    }

    /// Commit a transaction with view refresh
    pub fn commit_with_view_refresh(&self, transaction: graph_storage::Transaction) -> Result<(), MaterializedViewError> {
        self.storage_integration.commit_with_events(transaction)
    }

    /// Convert a DataChangeEvent to a DataChange for the incremental engine
    fn event_to_data_change(event: &DataChangeEvent) -> Option<DataChange> {
        match event {
            DataChangeEvent::VertexChanged { id } => {
                Some(DataChange::AddVertex {
                    id: *id,
                    properties: HashMap::new(),
                })
            }
            DataChangeEvent::VertexRemoved { id } => {
                Some(DataChange::RemoveVertex {
                    id: *id,
                    properties: HashMap::new(),
                })
            }
            DataChangeEvent::EdgeChanged { edge } => {
                Some(DataChange::AddEdge {
                    edge: edge.clone(),
                    properties: HashMap::new(),
                })
            }
            DataChangeEvent::EdgeRemoved { edge } => {
                Some(DataChange::RemoveEdge {
                    edge: edge.clone(),
                    properties: HashMap::new(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_storage::GraphStorage;
    use tempfile::tempdir;

    #[test]
    fn test_data_change_event() {
        let vertex_id = VertexId::new(1);
        let event = DataChangeEvent::VertexChanged { id: vertex_id };
        
        let affected = event.affected_vertices();
        assert_eq!(affected, vec![vertex_id]);
    }

    #[test]
    fn test_storage_integration() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let storage = Arc::new(GraphStorage::new(temp_dir.path())?);
        let integration = StorageIntegration::new(storage);

        let stats = integration.get_database_stats()?;
        assert_eq!(stats.vertex_count, 0);
        assert_eq!(stats.edge_count, 0);

        Ok(())
    }

    #[test]
    fn test_view_refresh_listener() {
        let listener = ViewRefreshListener::new("test-view".to_string());
        assert_eq!(listener.view_id(), "test-view");
    }

    #[test]
    fn test_integrated_view_manager() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let storage = Arc::new(GraphStorage::new(temp_dir.path())?);
        let storage_integration = Arc::new(StorageIntegration::new(storage));
        let manager = IntegratedViewManager::new(storage_integration);

        manager.register_view("test-view".to_string(), ViewType::Lookup { key_vertices: vec![] })?;

        Ok(())
    }
}