//! Simplified View Types for Graph Database
//!
//! This module provides core type definitions for materialized views
//! with simplified approach to avoid compilation issues.

use std::collections::HashMap;
use std::time::Duration;
use graph_core::{VertexId, PropertyValue, Properties};

#[cfg(feature = "sql")]
pub type SqlAst = Box<sqlparser::ast::Statement>;

/// View complexity classification
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Complexity {
    Simple,
    Moderate,
    Complex,
}

/// View size estimation metrics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ViewSizeMetrics {
    pub estimated_memory_bytes: usize,
    pub estimated_vertex_count: usize,
    pub estimated_edge_count: usize,
    pub update_frequency: u32,
}

impl Default for ViewSizeMetrics {
    fn default() -> Self {
        Self {
            estimated_memory_bytes: 1024,
            estimated_vertex_count: 0,
            estimated_edge_count: 0,
            update_frequency: 1,
        }
    }
}

/// Optimization hints for view execution
#[derive(Debug, Clone)]
pub enum OptimizationHint {
    PreferSpeed,
    PreferMemory,
    PreferFreshness,
    PreferAccuracy,
}

/// View type definitions
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ViewType {
    /// Fast key-value lookups
    Lookup {
        key_vertices: Vec<VertexId>,
    },
    /// Pre-computed aggregates
    Aggregation {
        aggregate_type: String,
    },
    /// Complex graph analytics
    Analytics {
        algorithm: String,
    },
    /// Hybrid combination of multiple types
    Hybrid {
        base_types: Vec<ViewType>,
    },
    /// SQL query-based view
    #[cfg(feature = "sql")]
    SqlQuery {
        query: String,
        #[serde(skip)]
        parsed_ast: Option<SqlAst>,
    },
}

/// Refresh policy for materialized views
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RefreshPolicy {
    /// Fixed interval refresh
    FixedInterval(Duration),
    /// Event-driven refresh
    EventDriven {
        debounce_ms: u64,
    },
    /// On-demand refresh
    OnDemand {
        ttl_ms: u64,
    },
    /// Hybrid strategy
    Hybrid {
        event_driven: bool,
        interval_ms: u64,
    },
}

/// Materialized view definition
#[derive(Debug, Clone)]
pub struct MaterializedView {
    /// Unique view identifier
    pub name: String,
    /// View type and configuration
    pub view_type: ViewType,
    /// Refresh policy
    pub refresh_policy: RefreshPolicy,
    /// Estimated size metrics
    pub size_metrics: ViewSizeMetrics,
    /// Optimization hints
    pub optimization_hints: Vec<OptimizationHint>,
    /// View creation timestamp
    pub created_at: std::time::SystemTime,
    /// Last refresh timestamp
    pub last_refreshed: std::time::SystemTime,
}

impl MaterializedView {
    /// Create a new materialized view
    pub fn new(
        name: String,
        view_type: ViewType,
        refresh_policy: RefreshPolicy,
        size_metrics: ViewSizeMetrics,
    ) -> Self {
        Self {
            name,
            view_type,
            refresh_policy,
            size_metrics,
            optimization_hints: vec![OptimizationHint::PreferSpeed],
            created_at: std::time::SystemTime::now(),
            last_refreshed: std::time::SystemTime::now(),
        }
    }

    /// Get a description of the view
    pub fn description(&self) -> String {
        match &self.view_type {
            ViewType::Lookup { key_vertices } => {
                format!("Lookup view for {} vertices", key_vertices.len())
            }
            ViewType::Aggregation { aggregate_type } => {
                format!("Aggregation view for {}", aggregate_type)
            }
            ViewType::Analytics { algorithm } => {
                format!("Analytics view using {}", algorithm)
            }
            ViewType::Hybrid { base_types } => {
                format!("Hybrid view combining {} base types", base_types.len())
            }
            #[cfg(feature = "sql")]
            ViewType::SqlQuery { query, .. } => {
                format!("SQL query view: {}", query)
            }
        }
    }

    /// Check if view needs refresh
    pub fn needs_refresh(&self) -> bool {
        match &self.refresh_policy {
            RefreshPolicy::FixedInterval(interval) => {
                std::time::SystemTime::now()
                    .duration_since(self.last_refreshed)
                    .unwrap_or(Duration::ZERO)
                    >= *interval
            }
            RefreshPolicy::EventDriven { .. } => {
                // Event-driven views refresh on data changes
                false
            }
            RefreshPolicy::OnDemand { ttl_ms } => {
                std::time::SystemTime::now()
                    .duration_since(self.last_refreshed)
                    .unwrap_or(Duration::ZERO)
                    .as_millis()
                    > *ttl_ms as u128
            }
            RefreshPolicy::Hybrid { event_driven, interval_ms } => {
                if *event_driven {
                    false // Event-driven handles it
                } else {
                    std::time::SystemTime::now()
                        .duration_since(self.last_refreshed)
                        .unwrap_or(Duration::ZERO)
                        .as_millis()
                        > *interval_ms as u128
                }
            }
        }
    }
}

/// Centrality types
#[derive(Debug, Clone, PartialEq)]
pub enum CentralityType {
    Degree,
    Betweenness,
    Closeness,
    PageRank,
    Eigenvector,
}

/// Query pattern matching
#[derive(Debug, Clone)]
pub struct QueryPattern {
    pub pattern_type: String,
    pub constraints: HashMap<String, PropertyValue>,
}

impl QueryPattern {
    /// Create a new query pattern
    pub fn new(pattern_type: String) -> Self {
        Self {
            pattern_type,
            constraints: HashMap::new(),
        }
    }

    /// Add constraint to pattern
    pub fn with_constraint(mut self, key: String, value: PropertyValue) -> Self {
        self.constraints.insert(key, value);
        self
    }

    /// Check if pattern matches given properties
    pub fn matches(&self, properties: &Properties) -> bool {
        self.constraints.iter().all(|(key, expected_value)| {
            properties
                .get(key)
                .map_or(false, |actual_value| actual_value == expected_value)
        })
    }
}

/// View status enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ViewStatus {
    Active,
    Refreshing,
    Error,
    Disabled,
}

/// Pattern type enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum PatternType {
    Triangle,
    Cycle,
    Star,
    Path { length: usize },
    Community,
}



impl Default for MaterializedView {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            view_type: ViewType::Lookup { key_vertices: vec![] },
            refresh_policy: RefreshPolicy::OnDemand { ttl_ms: 300000 },
            size_metrics: ViewSizeMetrics::default(),
            optimization_hints: vec![OptimizationHint::PreferSpeed],
            created_at: std::time::SystemTime::now(),
            last_refreshed: std::time::SystemTime::now(),
        }
    }
}

#[cfg(feature = "sql")]
impl ViewType {
    /// Create a SQL query view with parsed AST
    pub fn sql_query(query: String) -> Result<Self, String> {
        use crate::sql_parser::SqlParser;
        
        let parser = SqlParser::new();
        let parsed_ast = parser.parse(&query)
            .map_err(|e| e.to_string())?;
            
        parser.validate_for_view(&parsed_ast)
            .map_err(|e| e.to_string())?;
            
        Ok(ViewType::SqlQuery { 
            query, 
            parsed_ast: Some(parsed_ast),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_materialized_view_creation() {
        let view = MaterializedView::new(
            "test-view".to_string(),
            ViewType::Lookup { key_vertices: vec![VertexId::new(1), VertexId::new(2)] },
            RefreshPolicy::FixedInterval(Duration::from_secs(60)),
            ViewSizeMetrics::default(),
        );

        assert_eq!(view.name, "test-view");
        assert!(matches!(view.view_type, ViewType::Lookup { .. }));
    }

    #[test]
    fn test_query_pattern() {
        let pattern = QueryPattern::new("vertex-type".to_string())
            .with_constraint("type".to_string(), PropertyValue::String("user".to_string()));

        let mut properties = Properties::new();
        properties.insert("type".to_string(), PropertyValue::String("user".to_string()));

        assert!(pattern.matches(&properties));
    }

    #[test]
    fn test_view_refresh_policy() {
        let policy = RefreshPolicy::FixedInterval(Duration::from_secs(300));
        assert!(matches!(policy, RefreshPolicy::FixedInterval(_)));
    }

    #[test]
    fn test_view_needs_refresh() {
        let mut view = MaterializedView::new(
            "test-view".to_string(),
            ViewType::Lookup { key_vertices: vec![] },
            RefreshPolicy::OnDemand { ttl_ms: 1000 },
            ViewSizeMetrics::default(),
        );

        // Should need refresh initially
        assert!(view.needs_refresh());

        // Update last refresh time
        view.last_refreshed = std::time::SystemTime::now();

        // Should not need refresh immediately
        assert!(!view.needs_refresh());
    }
}