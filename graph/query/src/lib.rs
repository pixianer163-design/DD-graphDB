//! GQL (Graph Query Language) Parser and AST
//!
//! This module provides parsing and AST representation for GQL queries,
//! supporting Cypher-like syntax for graph database operations.

use std::collections::HashMap;
use graph_core::PropertyValue;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "pest")]
use pest::Parser;
#[cfg(feature = "pest")]
use pest_derive::Parser;

#[cfg(feature = "pest")]
#[derive(Parser)]
#[grammar = "gql.pest"]
pub struct GQLParser;

/// Represents a property value in GQL
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum GQLValue {
    String(String),
    Number(f64),
    Boolean(bool),
    Map(HashMap<String, GQLValue>),
    Null,
}

impl GQLValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            GQLValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_number(&self) -> Option<f64> {
        match self {
            GQLValue::Number(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            GQLValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&HashMap<String, GQLValue>> {
        match self {
            GQLValue::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, GQLValue::Null)
    }

    /// Convert to PropertyValue
    pub fn to_property_value(&self) -> PropertyValue {
        match self {
            GQLValue::String(s) => PropertyValue::String(s.clone()),
            GQLValue::Number(n) => {
                if n.fract() == 0.0 && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                    PropertyValue::Int64(*n as i64)
                } else {
                    PropertyValue::Float64(*n)
                }
            }
            GQLValue::Boolean(b) => PropertyValue::Bool(*b),
            GQLValue::Map(m) => {
                // For now, convert map to a simple string representation
                // This is a temporary workaround until PropertyValue gets a Map variant
                let map_str = m.iter()
                    .map(|(k, v)| format!("{}:{}", k, v.to_property_value().as_string().unwrap_or("null")))
                    .collect::<Vec<_>>()
                    .join(",");
                PropertyValue::String(format!("{{{}}}", map_str))
            }
            GQLValue::Null => PropertyValue::Null,
        }
    }
}

impl std::fmt::Display for GQLValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GQLValue::String(s) => write!(f, "\"{}\"", s),
            GQLValue::Number(n) => write!(f, "{}", n),
            GQLValue::Boolean(b) => write!(f, "{}", b),
            GQLValue::Null => write!(f, "null"),
            GQLValue::Map(m) => {
                write!(f, "{{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}: {}", k, v)?;
                }
                write!(f, "}}")
            }
        }
    }
}

/// Represents a node pattern in GQL
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NodePattern {
    pub variable: Option<String>,
    pub label: Option<String>,
    pub properties: HashMap<String, GQLValue>,
}

impl NodePattern {
    pub fn new() -> Self {
        Self {
            variable: None,
            label: None,
            properties: HashMap::new(),
        }
    }

    pub fn with_variable(mut self, var: String) -> Self {
        self.variable = Some(var);
        self
    }

    pub fn with_label(mut self, label: String) -> Self {
        self.label = Some(label);
        self
    }

    pub fn with_properties(mut self, props: HashMap<String, GQLValue>) -> Self {
        self.properties = props;
        self
    }
}

/// Represents an edge pattern in GQL
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct EdgePattern {
    pub variable: Option<String>,
    pub label: Option<String>,
    pub properties: HashMap<String, GQLValue>,
    pub direction: EdgeDirection,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum EdgeDirection {
    Outgoing,   // -> 
    Incoming,   // <-
    Undirected, // --
}

impl EdgePattern {
    pub fn new() -> Self {
        Self {
            variable: None,
            label: None,
            properties: HashMap::new(),
            direction: EdgeDirection::Outgoing,
        }
    }

    pub fn with_variable(mut self, var: String) -> Self {
        self.variable = Some(var);
        self
    }

    pub fn with_label(mut self, label: String) -> Self {
        self.label = Some(label);
        self
    }

    pub fn with_direction(mut self, dir: EdgeDirection) -> Self {
        self.direction = dir;
        self
    }
}

/// Represents a graph pattern (sequence of nodes and edges)
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct GraphPattern {
    pub nodes: Vec<NodePattern>,
    pub edges: Vec<EdgePattern>,
}

impl GraphPattern {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_node(mut self, node: NodePattern) -> Self {
        self.nodes.push(node);
        self
    }

    pub fn add_edge(mut self, edge: EdgePattern) -> Self {
        self.edges.push(edge);
        self
    }
}

/// Represents comparison operators in WHERE clauses
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    Greater,
    Less,
    GreaterEqual,
    LessEqual,
}

/// Represents logical operators in WHERE clauses
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum LogicalOp {
    And,
    Or,
}

/// Represents expressions in WHERE clauses
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Expression {
    Literal(GQLValue),
    PropertyAccess(String, String), // variable.property
    Comparison {
        left: Box<Expression>,
        operator: ComparisonOp,
        right: Box<Expression>,
    },
    Logical {
        left: Box<Expression>,
        operator: LogicalOp,
        right: Box<Expression>,
    },
}

impl Expression {
    pub fn literal(value: GQLValue) -> Self {
        Expression::Literal(value)
    }

    pub fn property(variable: String, property: String) -> Self {
        Expression::PropertyAccess(variable, property)
    }

    pub fn eq(left: Expression, right: Expression) -> Self {
        Expression::Comparison {
            left: Box::new(left),
            operator: ComparisonOp::Equal,
            right: Box::new(right),
        }
    }

    pub fn ne(left: Expression, right: Expression) -> Self {
        Expression::Comparison {
            left: Box::new(left),
            operator: ComparisonOp::NotEqual,
            right: Box::new(right),
        }
    }

    pub fn gt(left: Expression, right: Expression) -> Self {
        Expression::Comparison {
            left: Box::new(left),
            operator: ComparisonOp::Greater,
            right: Box::new(right),
        }
    }

    pub fn lt(left: Expression, right: Expression) -> Self {
        Expression::Comparison {
            left: Box::new(left),
            operator: ComparisonOp::Less,
            right: Box::new(right),
        }
    }

    pub fn and(left: Expression, right: Expression) -> Self {
        Expression::Logical {
            left: Box::new(left),
            operator: LogicalOp::And,
            right: Box::new(right),
        }
    }

    pub fn or(left: Expression, right: Expression) -> Self {
        Expression::Logical {
            left: Box::new(left),
            operator: LogicalOp::Or,
            right: Box::new(right),
        }
    }
}

/// Represents items in RETURN clause
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ReturnItem {
    Variable(String),
    Property(String, String), // variable.property
    All, // RETURN *
}

/// Represents a complete GQL statement
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Statement {
    Match {
        pattern: GraphPattern,
        where_clause: Option<Expression>,
        return_items: Vec<ReturnItem>,
    },
    Create {
        pattern: GraphPattern,
    },
    Delete {
        variable: String,
    },
    MatchDelete {
        pattern: GraphPattern,
        where_clause: Option<Expression>,
        variable: String,
    },
}

/// Main GQL parser implementation
#[cfg(feature = "pest")]
pub struct GQLParser;

#[cfg(feature = "pest")]
impl GQLParser {
    /// Parse a GQL query string into a Statement
    pub fn parse_query(input: &str) -> Result<Statement, String> {
        let pairs = GQLParser::parse(Rule::query, input)
            .map_err(|e| format!("Parse error at {}: {}", e.line_col, e))?;

        let pair = pairs.into_iter().next().unwrap();
        match pair.as_rule() {
            Rule::match_clause => Self::parse_match_statement(pair),
            Rule::create_clause => Self::parse_create_statement(pair),
            Rule::delete_clause => Self::parse_delete_statement(pair),
            _ => Err("Invalid query type".to_string()),
        }
    }

    fn parse_match_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement, String> {
        let mut pattern = None;
        let mut where_clause = None;
        let mut return_items = Vec::new();

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::pattern => {
                    pattern = Some(Self::parse_pattern(inner_pair)?);
                }
                Rule::where_clause => {
                    where_clause = Some(Self::parse_expression(inner_pair)?);
                }
                Rule::return_clause => {
                    return_items = Self::parse_return_clause(inner_pair)?;
                }
                _ => {}
            }
        }

        Ok(Statement::Match {
            pattern: pattern.unwrap(),
            where_clause,
            return_items,
        })
    }

    fn parse_create_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement, String> {
        let pattern = Self::parse_pattern(pair.into_inner().next().unwrap())?;
        Ok(Statement::Create { pattern })
    }

    fn parse_delete_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement, String> {
        let variable = pair.into_inner().next().unwrap().as_str().to_string();
        Ok(Statement::Delete { variable })
    }

    fn parse_pattern(pair: pest::iterators::Pair<Rule>) -> Result<GraphPattern, String> {
        let mut pattern = GraphPattern::new();
        let mut current_node: Option<NodePattern> = None;

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::node => {
                    let node = Self::parse_node(inner_pair)?;
                    if let Some(prev_node) = current_node {
                        pattern = pattern.add_node(prev_node);
                    }
                    current_node = Some(node);
                }
                Rule::edge => {
                    let edge = Self::parse_edge(inner_pair)?;
                    pattern = pattern.add_edge(edge);
                }
                _ => {}
            }
        }

        // Add the last node
        if let Some(node) = current_node {
            pattern = pattern.add_node(node);
        }

        Ok(pattern)
    }

    fn parse_node(pair: pest::iterators::Pair<Rule>) -> Result<NodePattern, String> {
        let mut node = NodePattern::new();

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::IDENTIFIER => {
                    if node.variable.is_none() {
                        node.variable = Some(inner_pair.as_str().to_string());
                    } else {
                        node.label = Some(inner_pair.as_str().to_string());
                    }
                }
                Rule::property_map => {
                    node.properties = Self::parse_properties(inner_pair)?;
                }
                _ => {}
            }
        }

        Ok(node)
    }

    fn parse_edge(pair: pest::iterators::Pair<Rule>) -> Result<EdgePattern, String> {
        let mut edge = EdgePattern::new();

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::IDENTIFIER => {
                    if edge.variable.is_none() {
                        edge.variable = Some(inner_pair.as_str().to_string());
                    } else {
                        edge.label = Some(inner_pair.as_str().to_string());
                    }
                }
                Rule::property_map => {
                    edge.properties = Self::parse_properties(inner_pair)?;
                }
                _ => {
                    // Determine direction from edge text
                    let text = inner_pair.as_str();
                    if text.contains("->") {
                        edge.direction = EdgeDirection::Outgoing;
                    } else if text.contains("<-") {
                        edge.direction = EdgeDirection::Incoming;
                    } else {
                        edge.direction = EdgeDirection::Undirected;
                    }
                }
            }
        }

        Ok(edge)
    }

    fn parse_properties(pair: pest::iterators::Pair<Rule>) -> Result<HashMap<String, GQLValue>, String> {
        let mut properties = HashMap::new();

        for inner_pair in pair.into_inner() {
            if inner_pair.as_rule() == Rule::property {
                let (key, value) = Self::parse_property(inner_pair)?;
                properties.insert(key, value);
            }
        }

        Ok(properties)
    }

    fn parse_property(pair: pest::iterators::Pair<Rule>) -> Result<(String, GQLValue), String> {
        let mut key = String::new();
        let mut value = GQLValue::Null;

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::IDENTIFIER => {
                    key = inner_pair.as_str().to_string();
                }
                Rule::value => {
                    value = Self::parse_value(inner_pair)?;
                }
                _ => {}
            }
        }

        Ok((key, value))
    }

    fn parse_value(pair: pest::iterators::Pair<Rule>) -> Result<GQLValue, String> {
        let inner_pair = pair.into_inner().next().unwrap();
        match inner_pair.as_rule() {
            Rule::STRING => {
                let s = inner_pair.as_str();
                Ok(GQLValue::String(s[1..s.len()-1].to_string())) // Remove quotes
            }
            Rule::NUMBER => {
                Ok(GQLValue::Number(inner_pair.as_str().parse().unwrap()))
            }
            Rule::BOOLEAN => {
                Ok(GQLValue::Boolean(inner_pair.as_str().parse().unwrap()))
            }
            Rule::property_map => {
                let properties = Self::parse_properties(inner_pair)?;
                Ok(GQLValue::Map(properties))
            }
            _ => Err("Invalid value".to_string()),
        }
    }

    fn parse_expression(pair: pest::iterators::Pair<Rule>) -> Result<Expression, String> {
        // This is a simplified implementation - a full expression parser would be more complex
        let mut expressions = Vec::new();

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::comparison => {
                    expressions.push(Self::parse_comparison(inner_pair)?);
                }
                _ => {}
            }
        }

        // For now, just return the first expression
        if let Some(expr) = expressions.into_iter().next() {
            Ok(expr)
        } else {
            Err("No expression found".to_string())
        }
    }

    fn parse_comparison(pair: pest::iterators::Pair<Rule>) -> Result<Expression, String> {
        let mut parts = Vec::new();

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::operand => {
                    parts.push(Self::parse_operand(inner_pair)?);
                }
                _ => {
                    let op_str = inner_pair.as_str();
                    let operator = match op_str {
                        "=" => ComparisonOp::Equal,
                        "!=" => ComparisonOp::NotEqual,
                        ">" => ComparisonOp::Greater,
                        "<" => ComparisonOp::Less,
                        _ => return Err(format!("Unknown operator: {}", op_str)),
                    };
                    parts.push(Expression::Literal(GQLValue::String(op_str.to_string())));
                }
            }
        }

        // Simplified parsing - assumes format: operand operator operand
        if parts.len() >= 3 {
            let operator_str = parts[1].as_string().unwrap();
            let operator = match operator_str.as_str() {
                "=" => ComparisonOp::Equal,
                "!=" => ComparisonOp::NotEqual,
                ">" => ComparisonOp::Greater,
                "<" => ComparisonOp::Less,
                _ => return Err(format!("Unknown operator: {}", operator_str)),
            };

            Ok(Expression::Comparison {
                left: Box::new(parts[0].clone()),
                operator,
                right: Box::new(parts[2].clone()),
            })
        } else {
            Err("Invalid comparison expression".to_string())
        }
    }

    fn parse_operand(pair: pest::iterators::Pair<Rule>) -> Result<Expression, String> {
        let text = pair.as_str();
        
        if let Some(dot_pos) = text.find('.') {
            let variable = text[..dot_pos].to_string();
            let property = text[dot_pos+1..].to_string();
            Ok(Expression::PropertyAccess(variable, property))
        } else {
            // Try to parse as different value types
            if text.starts_with('"') && text.ends_with('"') {
                Ok(Expression::Literal(GQLValue::String(text[1..text.len()-1].to_string())))
            } else if let Ok(num) = text.parse::<f64>() {
                Ok(Expression::Literal(GQLValue::Number(num)))
            } else if let Ok(b) = text.parse::<bool>() {
                Ok(Expression::Literal(GQLValue::Boolean(b)))
            } else {
                Ok(Expression::PropertyAccess(text.to_string(), "id".to_string()))
            }
        }
    }

    fn parse_return_clause(pair: pest::iterators::Pair<Rule>) -> Result<Vec<ReturnItem>, String> {
        let mut items = Vec::new();

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::return_item => {
                    let text = inner_pair.as_str();
                    if text == "*" {
                        items.push(ReturnItem::All);
                    } else if let Some(dot_pos) = text.find('.') {
                        let variable = text[..dot_pos].to_string();
                        let property = text[dot_pos+1..].to_string();
                        items.push(ReturnItem::Property(variable, property));
                    } else {
                        items.push(ReturnItem::Variable(text.to_string()));
                    }
                }
                _ => {}
            }
        }

        Ok(items)
    }
}

// When pest feature is not enabled, provide stub implementation
#[cfg(not(feature = "pest"))]
pub struct GQLParser;

#[cfg(not(feature = "pest"))]
impl GQLParser {
    pub fn parse_query(_input: &str) -> Result<Statement, String> {
        Err("GQL parsing requires 'pest' feature to be enabled".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gql_value_creation() {
        let string_val = GQLValue::String("hello".to_string());
        assert_eq!(string_val.as_string(), Some("hello"));

        let number_val = GQLValue::Number(42.5);
        assert_eq!(number_val.as_number(), Some(42.5));

        let bool_val = GQLValue::Boolean(true);
        assert_eq!(bool_val.as_bool(), Some(true));

        let null_val = GQLValue::Null;
        assert!(null_val.is_null());
    }

    #[test]
    fn test_node_pattern() {
        let node = NodePattern::new()
            .with_variable("v".to_string())
            .with_label("Person".to_string());

        assert_eq!(node.variable, Some("v".to_string()));
        assert_eq!(node.label, Some("Person".to_string()));
    }

    #[test]
    fn test_edge_pattern() {
        let edge = EdgePattern::new()
            .with_variable("e".to_string())
            .with_label("friend".to_string())
            .with_direction(EdgeDirection::Outgoing);

        assert_eq!(edge.variable, Some("e".to_string()));
        assert_eq!(edge.label, Some("friend".to_string()));
        assert_eq!(edge.direction, EdgeDirection::Outgoing);
    }

    #[test]
    fn test_expressions() {
        let expr1 = Expression::property("v".to_string(), "age".to_string());
        let expr2 = Expression::literal(GQLValue::Number(25.0));
        let comparison = Expression::gt(expr1, expr2);

        match comparison {
            Expression::Comparison { left, operator, right } => {
                assert_eq!(operator, ComparisonOp::Greater);
                assert!(matches!(*left, Expression::PropertyAccess(_, _)));
                assert!(matches!(*right, Expression::Literal(_)));
            }
            _ => panic!("Expected comparison expression"),
        }
    }

    #[test]
    fn test_gql_value_to_property() {
        let string_val = GQLValue::String("test".to_string());
        let prop_val = string_val.to_property_value();
        assert_eq!(prop_val.as_string(), Some("test"));

        let number_val = GQLValue::Number(42.0);
        let prop_val = number_val.to_property_value();
        assert_eq!(prop_val.as_int64(), Some(42));

        let float_val = GQLValue::Number(42.5);
        let prop_val = float_val.to_property_value();
        assert_eq!(prop_val.as_float64(), Some(42.5));
    }
}

/// Query execution engine
pub mod executor;

// Re-export executor types
pub use executor::{QueryExecutor, QueryResult, QueryError};