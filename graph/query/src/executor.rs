//! Query Execution Engine
//!
//! This module provides the query execution engine for GQL statements,
//! bridging the AST representation with the storage layer operations.

use std::collections::HashMap;
use std::sync::Arc;

use graph_core::{VertexId, Edge, Properties, PropertyValue};
use graph_storage::{GraphStorage, GraphOperation, StorageError};

use crate::{
    Statement, GraphPattern, NodePattern, EdgePattern, EdgeDirection,
    Expression, GQLValue, ComparisonOp, LogicalOp,
    ReturnItem,
};

/// Query execution error
#[derive(Debug, Clone)]
pub enum QueryError {
    SyntaxError(String),
    ExecutionError(String),
    NotImplemented(String),
    StorageError(String),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::SyntaxError(msg) => write!(f, "Syntax error: {}", msg),
            QueryError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            QueryError::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
            QueryError::StorageError(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}

impl std::error::Error for QueryError {}

impl From<StorageError> for QueryError {
    fn from(err: StorageError) -> Self {
        QueryError::StorageError(err.to_string())
    }
}

/// Query result
#[derive(Debug, Clone)]
pub enum QueryResult {
    Vertices(Vec<(VertexId, Properties)>),
    Edges(Vec<(Edge, Properties)>),
    Paths(Vec<Vec<VertexId>>),
    Values(Vec<Vec<(String, PropertyValue)>>),
    Empty,
}

impl QueryResult {
    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        match self {
            QueryResult::Vertices(v) => v.is_empty(),
            QueryResult::Edges(e) => e.is_empty(),
            QueryResult::Paths(p) => p.is_empty(),
            QueryResult::Values(v) => v.is_empty(),
            QueryResult::Empty => true,
        }
    }

    /// Get count of results
    pub fn count(&self) -> usize {
        match self {
            QueryResult::Vertices(v) => v.len(),
            QueryResult::Edges(e) => e.len(),
            QueryResult::Paths(p) => p.len(),
            QueryResult::Values(v) => v.len(),
            QueryResult::Empty => 0,
        }
    }

    /// Format result as JSON-like string
    pub fn format(&self) -> String {
        match self {
            QueryResult::Vertices(vertices) => {
                let formatted: Vec<String> = vertices
                    .iter()
                    .map(|(id, props)| {
                        let props_str = format_properties(props);
                        format!("{{\"id\": {}, \"properties\": {}}}", id.0, props_str)
                    })
                    .collect();
                format!("[{}]", formatted.join(", "))
            }
            QueryResult::Edges(edges) => {
                let formatted: Vec<String> = edges
                    .iter()
                    .map(|(edge, props)| {
                        let props_str = format_properties(props);
                        format!(
                            "{{\"src\": {}, \"dst\": {}, \"label\": \"{}\", \"properties\": {}}}",
                            edge.src.0, edge.dst.0, edge.label, props_str
                        )
                    })
                    .collect();
                format!("[{}]", formatted.join(", "))
            }
            QueryResult::Paths(paths) => {
                let formatted: Vec<String> = paths
                    .iter()
                    .map(|path| {
                        let ids: Vec<String> = path.iter().map(|id| id.0.to_string()).collect();
                        format!("[{}]", ids.join(", "))
                    })
                    .collect();
                format!("[{}]", formatted.join(", "))
            }
            QueryResult::Values(values) => {
                let formatted: Vec<String> = values
                    .iter()
                    .map(|row| {
                        let pairs: Vec<String> = row
                            .iter()
                            .map(|(name, value)| {
                                format!("\"{}\": \"{}\"", name, value_to_string(value))
                            })
                            .collect();
                        format!("{{{}}}", pairs.join(", "))
                    })
                    .collect();
                format!("[{}]", formatted.join(", "))
            }
            QueryResult::Empty => "[]".to_string(),
        }
    }
}

/// Helper function to format properties
fn format_properties(props: &Properties) -> String {
    let pairs: Vec<String> = props
        .iter()
        .map(|(k, v)| format!("\"{}\": \"{}\"", k, value_to_string(v)))
        .collect();
    format!("{{{}}}", pairs.join(", "))
}

/// Helper function to convert PropertyValue to string
fn value_to_string(value: &PropertyValue) -> String {
    match value {
        PropertyValue::String(s) => s.clone(),
        PropertyValue::Int64(i) => i.to_string(),
        PropertyValue::Float64(f) => f.to_string(),
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::Vec(v) => format!("[{}]", v.len()),
        PropertyValue::Null => "null".to_string(),
    }
}

/// Variable bindings during query execution
type Bindings = HashMap<String, (VertexId, Properties)>;

/// Query execution engine
pub struct QueryExecutor {
    storage: Arc<GraphStorage>,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(storage: Arc<GraphStorage>) -> Self {
        Self { storage }
    }

    /// Execute a query statement
    pub fn execute(&self, statement: Statement) -> Result<QueryResult, QueryError> {
        match statement {
            Statement::Match {
                pattern,
                where_clause,
                return_items,
            } => self.execute_match(&pattern, &where_clause, &return_items),
            Statement::Create { pattern } => {
                self.execute_create(&pattern)
            }
            Statement::Delete { variable: _ } => {
                Err(QueryError::NotImplemented("DELETE without MATCH not supported; use MATCH...DELETE".to_string()))
            }
            Statement::MatchDelete {
                pattern,
                where_clause,
                variable,
            } => self.execute_match_delete(&pattern, &where_clause, &variable),
        }
    }

    /// Execute a MATCH query
    fn execute_match(
        &self,
        pattern: &GraphPattern,
        where_clause: &Option<Expression>,
        return_items: &[ReturnItem],
    ) -> Result<QueryResult, QueryError> {
        // Match the pattern to get bindings
        let bindings = self.match_pattern(pattern)?;

        // Filter by WHERE clause
        let filtered_bindings: Vec<Bindings> = bindings
            .into_iter()
            .filter(|binding| {
                if let Some(expr) = where_clause {
                    self.evaluate_expression(expr, binding)
                } else {
                    true
                }
            })
            .collect();

        // Build result from RETURN items
        self.build_result(&filtered_bindings, return_items)
    }

    /// Match a graph pattern and return variable bindings
    fn match_pattern(&self, pattern: &GraphPattern) -> Result<Vec<Bindings>, QueryError> {
        if pattern.nodes.is_empty() {
            return Ok(vec![]);
        }

        let mut all_bindings: Vec<Bindings> = vec![HashMap::new()];

        // Start with the first node
        let first_node = &pattern.nodes[0];
        let matching_vertices = self.find_matching_vertices(first_node)?;

        all_bindings = matching_vertices
            .into_iter()
            .map(|(id, props)| {
                let mut binding = HashMap::new();
                if let Some(var) = &first_node.variable {
                    binding.insert(var.clone(), (id, props));
                }
                binding
            })
            .collect();

        // Process edges and subsequent nodes
        for (i, edge_pattern) in pattern.edges.iter().enumerate() {
            let next_node = &pattern.nodes[i + 1];
            let mut new_bindings: Vec<Bindings> = Vec::new();

            for binding in &all_bindings {
                // Get the current node from binding
                let current_var = pattern.nodes[i].variable.as_ref().unwrap();
                let (current_id, _) = binding.get(current_var).unwrap();

                // Find matching edges
                let matching_edges =
                    self.find_matching_edges(*current_id, edge_pattern, next_node)?;

                for (_edge, target_id, target_props) in matching_edges {
                    let mut new_binding = binding.clone();

                    // Add edge to binding
                    if let Some(_edge_var) = &edge_pattern.variable {
                        // Store edge as a special property in the binding
                        // For now, we'll handle this differently
                    }

                    // Add target node to binding
                    if let Some(node_var) = &next_node.variable {
                        new_binding.insert(node_var.clone(), (target_id, target_props));
                    }

                    new_bindings.push(new_binding);
                }
            }

            all_bindings = new_bindings;
        }

        Ok(all_bindings)
    }

    /// Find vertices matching a node pattern
    fn find_matching_vertices(
        &self,
        node: &NodePattern,
    ) -> Result<Vec<(VertexId, Properties)>, QueryError> {
        let all_vertices = self.storage.list_vertices()?;

        let matching: Vec<(VertexId, Properties)> = all_vertices
            .into_iter()
            .filter(|(_id, props)| {
                // Check label match
                if let Some(label) = &node.label {
                    let type_prop = props.get("type").and_then(|p| p.as_string());
                    if type_prop != Some(label.as_str()) {
                        return false;
                    }
                }

                // Check property matches
                for (key, value) in &node.properties {
                    if let Some(prop_value) = props.get(key) {
                        if !gql_value_matches_property(value, prop_value) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }

                true
            })
            .collect();

        Ok(matching)
    }

    /// Find edges matching a pattern from a starting vertex
    fn find_matching_edges(
        &self,
        start_id: VertexId,
        edge_pattern: &EdgePattern,
        target_node: &NodePattern,
    ) -> Result<Vec<(Edge, VertexId, Properties)>, QueryError> {
        let neighbors = match edge_pattern.direction {
            EdgeDirection::Outgoing => self.storage.get_out_neighbors(start_id)?,
            EdgeDirection::Incoming => self.storage.get_in_neighbors(start_id)?,
            EdgeDirection::Undirected => self.storage.get_all_neighbors(start_id)?,
        };

        let matching: Vec<(Edge, VertexId, Properties)> = neighbors
            .into_iter()
            .filter(|(target_id, edge, _)| {
                // Check edge label match
                if let Some(label) = &edge_pattern.label {
                    if &edge.label != label {
                        return false;
                    }
                }

                // Check edge properties
                // Note: edge properties are not currently stored in get_out_neighbors result
                // This is a limitation to be addressed

                // Check target node match
                if let Ok(Some(target_props)) = self.storage.get_vertex(*target_id) {
                    // Check target node label
                    if let Some(label) = &target_node.label {
                        let type_prop = target_props.get("type").and_then(|p| p.as_string());
                        if type_prop != Some(label.as_str()) {
                            return false;
                        }
                    }

                    // Check target node properties
                    for (key, value) in &target_node.properties {
                        if let Some(prop_value) = target_props.get(key) {
                            if !gql_value_matches_property(value, prop_value) {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }

                    true
                } else {
                    false
                }
            })
            .map(|(target_id, edge, _)| {
                let target_props = self
                    .storage
                    .get_vertex(target_id)
                    .unwrap()
                    .unwrap_or_default();
                (edge, target_id, target_props)
            })
            .collect();

        Ok(matching)
    }

    /// Evaluate a WHERE expression
    fn evaluate_expression(&self, expr: &Expression, bindings: &Bindings) -> bool {
        match expr {
            Expression::Literal(value) => match value {
                GQLValue::Boolean(b) => *b,
                _ => false,
            },
            Expression::PropertyAccess(var, prop) => {
                if let Some((_, props)) = bindings.get(var) {
                    if let Some(value) = props.get(prop) {
                        return matches!(value, PropertyValue::Bool(true));
                    }
                }
                false
            }
            Expression::Comparison {
                left,
                operator,
                right,
            } => self.evaluate_comparison(left, operator, right, bindings),
            Expression::Logical { left, operator, right } => {
                let left_result = self.evaluate_expression(left, bindings);
                let right_result = self.evaluate_expression(right, bindings);

                match operator {
                    LogicalOp::And => left_result && right_result,
                    LogicalOp::Or => left_result || right_result,
                }
            }
        }
    }

    /// Evaluate a comparison expression
    fn evaluate_comparison(
        &self,
        left: &Expression,
        operator: &ComparisonOp,
        right: &Expression,
        bindings: &Bindings,
    ) -> bool {
        let left_value = self.get_expression_value(left, bindings);
        let right_value = self.get_expression_value(right, bindings);

        match (left_value, right_value) {
            (Some(lv), Some(rv)) => match operator {
                ComparisonOp::Equal => compare_values(&lv, &rv) == Some(std::cmp::Ordering::Equal),
                ComparisonOp::NotEqual => {
                    compare_values(&lv, &rv) != Some(std::cmp::Ordering::Equal)
                }
                ComparisonOp::Greater => {
                    compare_values(&lv, &rv) == Some(std::cmp::Ordering::Greater)
                }
                ComparisonOp::Less => {
                    compare_values(&lv, &rv) == Some(std::cmp::Ordering::Less)
                }
                ComparisonOp::GreaterEqual => {
                    let cmp = compare_values(&lv, &rv);
                    cmp == Some(std::cmp::Ordering::Greater)
                        || cmp == Some(std::cmp::Ordering::Equal)
                }
                ComparisonOp::LessEqual => {
                    let cmp = compare_values(&lv, &rv);
                    cmp == Some(std::cmp::Ordering::Less)
                        || cmp == Some(std::cmp::Ordering::Equal)
                }
            },
            _ => false,
        }
    }

    /// Get value from expression
    fn get_expression_value(
        &self,
        expr: &Expression,
        bindings: &Bindings,
    ) -> Option<PropertyValue> {
        match expr {
            Expression::Literal(gql_value) => Some(gql_value.to_property_value()),
            Expression::PropertyAccess(var, prop) => {
                bindings.get(var).and_then(|(_, props)| props.get(prop).cloned())
            }
            _ => None,
        }
    }

    /// Build result from bindings and RETURN items
    fn build_result(
        &self,
        bindings: &[Bindings],
        return_items: &[ReturnItem],
    ) -> Result<QueryResult, QueryError> {
        if bindings.is_empty() {
            return Ok(QueryResult::Empty);
        }

        // If returning all (*), return all bound vertices
        if return_items.is_empty() || return_items.iter().any(|item| matches!(item, ReturnItem::All)) {
            let vertices: Vec<(VertexId, Properties)> = bindings
                .iter()
                .flat_map(|binding| binding.values().cloned().collect::<Vec<_>>())
                .collect();
            return Ok(QueryResult::Vertices(vertices));
        }

        // Build values from RETURN items
        let mut rows: Vec<Vec<(String, PropertyValue)>> = Vec::new();

        for binding in bindings {
            let mut row: Vec<(String, PropertyValue)> = Vec::new();

            for item in return_items {
                match item {
                    ReturnItem::Variable(var) => {
                        if let Some((_id, props)) = binding.get(var) {
                            // Return vertex properties
                            for (key, value) in props {
                                row.push((format!("{}.{}", var, key), value.clone()));
                            }
                        }
                    }
                    ReturnItem::Property(var, prop) => {
                        if let Some((_, props)) = binding.get(var) {
                            if let Some(value) = props.get(prop) {
                                row.push((format!("{}.{}", var, prop), value.clone()));
                            }
                        }
                    }
                    ReturnItem::All => {
                        // Already handled above
                    }
                }
            }

            rows.push(row);
        }

        Ok(QueryResult::Values(rows))
    }

    /// Execute a CREATE statement
    fn execute_create(&self, pattern: &GraphPattern) -> Result<QueryResult, QueryError> {
        let mut transaction = self.storage.begin_transaction()?;
        let mut created_vertices = Vec::new();

        // Create vertices from node patterns
        for node in &pattern.nodes {
            // Generate a vertex ID (use hash of label+properties as a simple strategy)
            let id = self.next_vertex_id()?;

            let mut properties: Properties = node.properties.iter()
                .map(|(k, v)| (k.clone(), v.to_property_value()))
                .collect();

            // Store label as a "type" property
            if let Some(label) = &node.label {
                properties.insert("type".to_string(), PropertyValue::String(label.clone()));
            }

            transaction.add_operation(GraphOperation::AddVertex {
                id,
                properties: properties.clone(),
            });
            created_vertices.push((id, properties));
        }

        // Create edges from edge patterns (connect consecutive nodes)
        for (i, edge_pattern) in pattern.edges.iter().enumerate() {
            if i + 1 < created_vertices.len() {
                let src = created_vertices[i].0;
                let dst = created_vertices[i + 1].0;
                let label = edge_pattern.label.clone().unwrap_or_else(|| "related".to_string());

                let properties: Properties = edge_pattern.properties.iter()
                    .map(|(k, v)| (k.clone(), v.to_property_value()))
                    .collect();

                transaction.add_operation(GraphOperation::AddEdge {
                    edge: Edge::new(src, dst, label),
                    properties,
                });
            }
        }

        self.storage.commit_transaction(transaction)?;
        Ok(QueryResult::Vertices(created_vertices))
    }

    /// Execute a MATCH...DELETE statement
    fn execute_match_delete(
        &self,
        pattern: &GraphPattern,
        where_clause: &Option<Expression>,
        variable: &str,
    ) -> Result<QueryResult, QueryError> {
        // First, match to find vertices to delete
        let bindings = self.match_pattern(pattern)?;

        let filtered_bindings: Vec<Bindings> = bindings
            .into_iter()
            .filter(|binding| {
                if let Some(expr) = where_clause {
                    self.evaluate_expression(expr, binding)
                } else {
                    true
                }
            })
            .collect();

        // Collect vertex IDs to delete
        let mut to_delete: Vec<VertexId> = Vec::new();
        for binding in &filtered_bindings {
            if let Some((id, _)) = binding.get(variable) {
                to_delete.push(*id);
            }
        }

        if to_delete.is_empty() {
            return Ok(QueryResult::Empty);
        }

        // Delete matched vertices
        let mut transaction = self.storage.begin_transaction()?;
        let mut deleted = Vec::new();

        for id in &to_delete {
            // Get vertex properties before deletion for the result
            if let Ok(Some(props)) = self.storage.get_vertex(*id) {
                transaction.add_operation(GraphOperation::RemoveVertex { id: *id });
                deleted.push((*id, props));
            }
        }

        self.storage.commit_transaction(transaction)?;
        Ok(QueryResult::Vertices(deleted))
    }

    /// Generate the next available vertex ID
    fn next_vertex_id(&self) -> Result<VertexId, QueryError> {
        let vertices = self.storage.list_vertices()?;
        let max_id = vertices.iter()
            .map(|(id, _)| id.value())
            .max()
            .unwrap_or(0);
        Ok(VertexId::new(max_id + 1))
    }
}

/// Check if a GQL value matches a property value
fn gql_value_matches_property(gql_value: &GQLValue, property: &PropertyValue) -> bool {
    match (gql_value, property) {
        (GQLValue::String(s1), PropertyValue::String(s2)) => s1 == s2,
        (GQLValue::Number(n), PropertyValue::Int64(i)) => *n == *i as f64,
        (GQLValue::Number(n), PropertyValue::Float64(f)) => *n == *f,
        (GQLValue::Boolean(b1), PropertyValue::Bool(b2)) => b1 == b2,
        (GQLValue::Null, PropertyValue::Null) => true,
        _ => false,
    }
}

/// Compare two property values
fn compare_values(left: &PropertyValue, right: &PropertyValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (PropertyValue::Int64(l), PropertyValue::Int64(r)) => l.partial_cmp(r),
        (PropertyValue::Float64(l), PropertyValue::Float64(r)) => l.partial_cmp(r),
        (PropertyValue::Int64(l), PropertyValue::Float64(r)) => (*l as f64).partial_cmp(r),
        (PropertyValue::Float64(l), PropertyValue::Int64(r)) => l.partial_cmp(&(*r as f64)),
        (PropertyValue::String(l), PropertyValue::String(r)) => l.partial_cmp(r),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_core::props;
    use graph_storage::GraphOperation;

    fn make_person(name: &str, age: i64) -> Properties {
        let mut props = HashMap::new();
        props.insert("name".to_string(), PropertyValue::String(name.to_string()));
        props.insert("age".to_string(), PropertyValue::Int64(age));
        props.insert("type".to_string(), PropertyValue::String("Person".to_string()));
        props
    }

    fn make_person_with_dept(name: &str, age: i64, department: &str) -> Properties {
        let mut props = make_person(name, age);
        props.insert("department".to_string(), PropertyValue::String(department.to_string()));
        props
    }

    fn make_edge_props(since: i64) -> Properties {
        let mut props = HashMap::new();
        props.insert("since".to_string(), PropertyValue::Int64(since));
        props
    }

    fn create_test_storage() -> Arc<GraphStorage> {
        let temp_dir = std::env::temp_dir().join("query_test");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = Arc::new(GraphStorage::new(&temp_dir).unwrap());

        // Create test data
        let mut transaction = storage.begin_transaction().unwrap();

        // Add vertices
        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(1),
            properties: make_person("Alice", 30),
        });
        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(2),
            properties: make_person("Bob", 25),
        });
        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(3),
            properties: make_person("Charlie", 35),
        });

        // Add edges
        transaction.add_operation(GraphOperation::AddEdge {
            edge: Edge::new(VertexId::new(1), VertexId::new(2), "friend"),
            properties: make_edge_props(2020),
        });
        transaction.add_operation(GraphOperation::AddEdge {
            edge: Edge::new(VertexId::new(2), VertexId::new(3), "friend"),
            properties: make_edge_props(2021),
        });

        storage.commit_transaction(transaction).unwrap();

        storage
    }

    #[test]
    fn test_query_executor_creation() {
        let storage = create_test_storage();
        let _executor = QueryExecutor::new(storage);
        // Just verify it doesn't panic
    }

    #[test]
    fn test_query_result_empty() {
        let result = QueryResult::Empty;
        assert!(result.is_empty());
        assert_eq!(result.count(), 0);
    }

    #[test]
    fn test_query_result_vertices() {
        let vertices = vec![
            (VertexId::new(1), props::map(vec![("name", "Alice")])),
        ];
        let result = QueryResult::Vertices(vertices);
        assert!(!result.is_empty());
        assert_eq!(result.count(), 1);
    }

    #[test]
    fn test_property_filter_query() {
        let storage = create_test_storage();
        let executor = QueryExecutor::new(storage);

        // MATCH (v:Person) WHERE v.age > 25 RETURN v.name, v.age
        let pattern = GraphPattern::new()
            .add_node(
                NodePattern::new()
                    .with_variable("v".to_string())
                    .with_label("Person".to_string()),
            );

        let where_clause = Some(Expression::gt(
            Expression::property("v".to_string(), "age".to_string()),
            Expression::literal(GQLValue::Number(25.0)),
        ));

        let return_items = vec![
            ReturnItem::Property("v".to_string(), "name".to_string()),
            ReturnItem::Property("v".to_string(), "age".to_string()),
        ];

        let statement = Statement::Match {
            pattern,
            where_clause,
            return_items,
        };

        let result = executor.execute(statement).unwrap();

        // Alice (30) and Charlie (35) should match, Bob (25) should not
        assert_eq!(result.count(), 2);

        if let QueryResult::Values(rows) = &result {
            let names: Vec<String> = rows
                .iter()
                .filter_map(|row| {
                    row.iter()
                        .find(|(k, _)| k == "v.name")
                        .and_then(|(_, v)| v.as_string().map(|s| s.to_string()))
                })
                .collect();
            assert!(names.contains(&"Alice".to_string()));
            assert!(names.contains(&"Charlie".to_string()));
            assert!(!names.contains(&"Bob".to_string()));
        } else {
            panic!("Expected Values result, got {:?}", result);
        }
    }

    #[test]
    fn test_edge_traversal_query() {
        let storage = create_test_storage();
        let executor = QueryExecutor::new(storage);

        // MATCH (a)-[e:friend]->(b) RETURN b.name
        // Graph: Alice->Bob->Charlie via "friend" edges
        let pattern = GraphPattern::new()
            .add_node(
                NodePattern::new()
                    .with_variable("a".to_string()),
            )
            .add_edge(
                EdgePattern::new()
                    .with_variable("e".to_string())
                    .with_label("friend".to_string())
                    .with_direction(EdgeDirection::Outgoing),
            )
            .add_node(
                NodePattern::new()
                    .with_variable("b".to_string()),
            );

        let statement = Statement::Match {
            pattern,
            where_clause: None,
            return_items: vec![ReturnItem::Property("b".to_string(), "name".to_string())],
        };

        let result = executor.execute(statement).unwrap();

        // Two friend edges: Alice->Bob and Bob->Charlie
        // So b should be Bob and Charlie
        assert_eq!(result.count(), 2);

        if let QueryResult::Values(rows) = &result {
            let names: Vec<String> = rows
                .iter()
                .filter_map(|row| {
                    row.iter()
                        .find(|(k, _)| k == "b.name")
                        .and_then(|(_, v)| v.as_string().map(|s| s.to_string()))
                })
                .collect();
            assert!(names.contains(&"Bob".to_string()));
            assert!(names.contains(&"Charlie".to_string()));
            assert!(!names.contains(&"Alice".to_string()));
        } else {
            panic!("Expected Values result, got {:?}", result);
        }
    }

    fn create_test_storage_with_departments() -> Arc<GraphStorage> {
        let temp_dir = std::env::temp_dir().join("query_test_dept");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let storage = Arc::new(GraphStorage::new(&temp_dir).unwrap());

        let mut transaction = storage.begin_transaction().unwrap();

        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(1),
            properties: make_person_with_dept("Alice", 30, "Engineering"),
        });
        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(2),
            properties: make_person_with_dept("Bob", 25, "Engineering"),
        });
        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(3),
            properties: make_person_with_dept("Charlie", 35, "Marketing"),
        });
        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(4),
            properties: make_person_with_dept("Diana", 28, "Engineering"),
        });

        storage.commit_transaction(transaction).unwrap();
        storage
    }

    #[test]
    fn test_multi_condition_query() {
        let storage = create_test_storage_with_departments();
        let executor = QueryExecutor::new(storage);

        // MATCH (v:Person) WHERE v.age > 25 AND v.department = 'Engineering' RETURN v.name
        let pattern = GraphPattern::new()
            .add_node(
                NodePattern::new()
                    .with_variable("v".to_string())
                    .with_label("Person".to_string()),
            );

        let where_clause = Some(Expression::and(
            Expression::gt(
                Expression::property("v".to_string(), "age".to_string()),
                Expression::literal(GQLValue::Number(25.0)),
            ),
            Expression::eq(
                Expression::property("v".to_string(), "department".to_string()),
                Expression::literal(GQLValue::String("Engineering".to_string())),
            ),
        ));

        let statement = Statement::Match {
            pattern,
            where_clause,
            return_items: vec![ReturnItem::Property("v".to_string(), "name".to_string())],
        };

        let result = executor.execute(statement).unwrap();

        // Alice (30, Engineering) and Diana (28, Engineering) match age > 25 AND dept = Engineering
        // Bob (25, Engineering) fails age > 25
        // Charlie (35, Marketing) fails dept = Engineering
        assert_eq!(result.count(), 2);

        if let QueryResult::Values(rows) = &result {
            let names: Vec<String> = rows
                .iter()
                .filter_map(|row| {
                    row.iter()
                        .find(|(k, _)| k == "v.name")
                        .and_then(|(_, v)| v.as_string().map(|s| s.to_string()))
                })
                .collect();
            assert!(names.contains(&"Alice".to_string()));
            assert!(names.contains(&"Diana".to_string()));
            assert!(!names.contains(&"Bob".to_string()));
            assert!(!names.contains(&"Charlie".to_string()));
        } else {
            panic!("Expected Values result, got {:?}", result);
        }
    }
}
