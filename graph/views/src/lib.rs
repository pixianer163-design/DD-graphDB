//! Graph Views Module - materialized view support for the graph database.

pub mod view_types;
pub mod storage_integration;
pub mod query_router;
pub mod multilevel_cache;
pub mod cache_manager;
pub mod differential_dataflow;
pub mod incremental_engine;
pub mod stream_processing;
pub mod windowed_operations;
pub mod view_registry;
#[cfg(feature = "sql")]
pub mod sql_parser;

// Re-export core types
pub use view_types::{
    MaterializedView, ViewType, RefreshPolicy, ViewSizeMetrics,
    Complexity, PatternType, OptimizationHint
};

pub use storage_integration::{
    StorageIntegration, DataChangeEvent, ViewDataReader, DataChangeListener,
    ViewRefreshListener, IntegratedViewManager
};

pub use query_router::{
    QueryRouter, QueryPattern, RoutingDecision, ViewPerformanceStats
};

pub use multilevel_cache::{
    Cache, CacheEntry, CacheConfig, CacheStats, ReplacementPolicy,
    MultiLevelCacheStats, create_cache
};

pub use cache_manager::{
    MultiLevelCacheManager, MultiLevelCacheConfig, ViewCacheData, ViewCacheEntry,
    CacheLevelConfig
};

pub use differential_dataflow::{
    DataChange, ChangeSet, DifferentialUpdate, ViewDependency, DependencyType,
    DependencyGraph, IncrementalConfig
};

pub use incremental_engine::{
    IncrementalEngine, EngineState, UpdateResult
};

pub use stream_processing::{
    StreamEvent, StreamEventType, StreamEventData, StreamBufferConfig, StreamBuffer,
    StreamError, StreamProcessorConfig, StreamProcessor, StreamProcessorStats,
};

pub use windowed_operations::{
    WindowSpec, WindowType, WindowResult, WindowState, AggregationFunction,
    WindowManager, WindowStats, WindowError
};

pub use view_registry::{ViewRegistry, ViewStore, ViewDefinition};

/// Error types for views module
#[derive(Debug, thiserror::Error)]
pub enum ViewError {
    #[error("Invalid view definition: {details}")]
    InvalidDefinition { details: String },
    
    #[error("Storage operation failed: {error}")]
    StorageError { error: String },
    
    #[error("View {view_name} not found")]
    ViewNotFound { view_name: String },
    
    #[error("Invalid refresh policy: {policy}")]
    InvalidRefreshPolicy { policy: String },
    
    #[error("View dependency cycle detected: {cycle}")]
    DependencyCycle { cycle: String },
    
    #[error("Query execution failed: {query}")]
    QueryExecutionFailed { query: String },
    
    #[error("Incremental update failed: {operation}")]
    IncrementalUpdateFailed { operation: String },
    
    #[error("View refresh failed: {view_name}")]
    RefreshFailed { view_name: String },
    
    #[error("Memory allocation failed")]
    MemoryAllocationFailed { size: usize },
    
    #[error("View {view_name} is in invalid state")]
    InvalidState { view_name: String, state: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_view_type_creation() {
        // Test that all view types can be created
        let lookup_view = ViewType::Lookup { key_vertices: vec![] };
        assert!(matches!(lookup_view, ViewType::Lookup { .. }));
    }
    
    #[test]
    fn test_refresh_policy_validation() {
        // Test refresh policy validation
        let policy = RefreshPolicy::FixedInterval(std::time::Duration::from_secs(60));
        assert!(matches!(policy, RefreshPolicy::FixedInterval(_)));
    }
}