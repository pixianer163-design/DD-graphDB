//! Graph Database Core Data Types
//! 
//! This module provides the foundational data structures for the graph database,
//! including vertices, edges, and properties. These types are designed to be
//! serializable, hashable, and efficient for differential dataflow operations.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Unique identifier for a vertex in the graph
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct VertexId(pub u64);

impl VertexId {
    /// Create a new vertex ID
    pub fn new(id: u64) -> Self {
        VertexId(id)
    }
    
    /// Get the raw numeric value
    pub fn value(&self) -> u64 {
        self.0
    }
    
    /// Create a vertex ID from a string representation
    pub fn from_str(s: &str) -> Result<Self, std::num::ParseIntError> {
        Ok(VertexId(s.parse()?))
    }
}

impl std::fmt::Display for VertexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for VertexId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(VertexId(s.parse()?))
    }
}

impl From<u64> for VertexId {
    fn from(id: u64) -> Self {
        VertexId(id)
    }
}

impl From<VertexId> for u64 {
    fn from(id: VertexId) -> Self {
        id.0
    }
}

/// Directed edge in the graph connecting a source vertex to a destination vertex
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Edge {
    /// Source vertex ID
    pub src: VertexId,
    /// Destination vertex ID  
    pub dst: VertexId,
    /// Edge label/type
    pub label: String,
}

impl Edge {
    /// Create a new edge
    pub fn new(src: VertexId, dst: VertexId, label: impl Into<String>) -> Self {
        Edge {
            src,
            dst,
            label: label.into(),
        }
    }
    
    /// Create an edge with numeric IDs
    pub fn from_ids(src: u64, dst: u64, label: impl Into<String>) -> Self {
        Edge::new(VertexId(src), VertexId(dst), label)
    }
    
    /// Get the reversed edge (swap src and dst)
    pub fn reversed(&self) -> Self {
        Edge {
            src: self.dst,
            dst: self.src,
            label: self.label.clone(),
        }
    }
    
    /// Check if this edge connects the same vertices as another (ignoring direction)
    pub fn connects_same_vertices(&self, other: &Edge) -> bool {
        (self.src == other.src && self.dst == other.dst) ||
        (self.src == other.dst && self.dst == other.src)
    }
}

/// Property value that can be stored on vertices and edges
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PropertyValue {
    /// String value
    String(String),
    /// 64-bit integer value
    Int64(i64),
    /// 64-bit floating point value
    Float64(f64),
    /// Boolean value
    Bool(bool),
    /// Vector of property values
    Vec(Vec<PropertyValue>),
    /// Null value
    Null,
}

// Custom PartialEq: treats NaN == NaN via bit comparison so that
// Hash contract is satisfied (a == b => hash(a) == hash(b))
impl PartialEq for PropertyValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PropertyValue::String(a), PropertyValue::String(b)) => a == b,
            (PropertyValue::Int64(a), PropertyValue::Int64(b)) => a == b,
            (PropertyValue::Float64(a), PropertyValue::Float64(b)) => a.to_bits() == b.to_bits(),
            (PropertyValue::Bool(a), PropertyValue::Bool(b)) => a == b,
            (PropertyValue::Vec(a), PropertyValue::Vec(b)) => a == b,
            (PropertyValue::Null, PropertyValue::Null) => true,
            _ => false,
        }
    }
}

impl Eq for PropertyValue {}

impl PropertyValue {
    /// Create a string property
    pub fn string(value: impl Into<String>) -> Self {
        PropertyValue::String(value.into())
    }
    
    /// Create an integer property
    pub fn int64(value: i64) -> Self {
        PropertyValue::Int64(value)
    }
    
    /// Create a float property
    pub fn float64(value: f64) -> Self {
        PropertyValue::Float64(value)
    }
    
    /// Create a boolean property
    pub fn bool(value: bool) -> Self {
        PropertyValue::Bool(value)
    }
    
    /// Create a vector property
    pub fn vec(values: Vec<PropertyValue>) -> Self {
        PropertyValue::Vec(values)
    }
    
    /// Get the string value, if applicable
    pub fn as_string(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s),
            _ => None,
        }
    }
    
    /// Get the integer value, if applicable
    pub fn as_int64(&self) -> Option<i64> {
        match self {
            PropertyValue::Int64(i) => Some(*i),
            _ => None,
        }
    }
    
    /// Get the float value, if applicable
    pub fn as_float64(&self) -> Option<f64> {
        match self {
            PropertyValue::Float64(f) => Some(*f),
            PropertyValue::Int64(i) => Some(*i as f64), // Allow int to float conversion
            _ => None,
        }
    }
    
    /// Get the boolean value, if applicable
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            PropertyValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
    
    /// Get the vector value, if applicable
    pub fn as_vec(&self) -> Option<&Vec<PropertyValue>> {
        match self {
            PropertyValue::Vec(v) => Some(v),
            _ => None,
        }
    }
    
    /// Check if the property is null
    pub fn is_null(&self) -> bool {
        matches!(self, PropertyValue::Null)
    }
    
    /// Get the type name of this property value
    pub fn type_name(&self) -> &'static str {
        match self {
            PropertyValue::String(_) => "string",
            PropertyValue::Int64(_) => "int64",
            PropertyValue::Float64(_) => "float64",
            PropertyValue::Bool(_) => "bool",
            PropertyValue::Vec(_) => "vec",
            PropertyValue::Null => "null",
        }
    }
}

impl std::fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropertyValue::String(s) => write!(f, "\"{}\"", s),
            PropertyValue::Int64(i) => write!(f, "{}", i),
            PropertyValue::Float64(fl) => write!(f, "{}", fl),
            PropertyValue::Bool(b) => write!(f, "{}", b),
            PropertyValue::Vec(v) => {
                write!(f, "[")?;
                for (i, val) in v.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", val)?;
                }
                write!(f, "]")
            }
            PropertyValue::Null => write!(f, "null"),
        }
    }
}

// Implement Hash for PropertyValue to allow use in HashMap keys
impl Hash for PropertyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            PropertyValue::String(s) => {
                0u8.hash(state);
                s.hash(state);
            }
            PropertyValue::Int64(i) => {
                1u8.hash(state);
                i.hash(state);
            }
            PropertyValue::Float64(f) => {
                2u8.hash(state);
                // Canonicalize NaN to a single bit pattern so all NaN values
                // hash identically (required by Hash contract: a == b => hash(a) == hash(b))
                let bits = if f.is_nan() { f64::NAN.to_bits() } else { f.to_bits() };
                bits.hash(state);
            }
            PropertyValue::Bool(b) => {
                3u8.hash(state);
                b.hash(state);
            }
            PropertyValue::Vec(v) => {
                4u8.hash(state);
                for item in v {
                    item.hash(state);
                }
            }
            PropertyValue::Null => {
                5u8.hash(state);
            }
        }
    }
}

/// Type alias for a collection of properties
pub type Properties = HashMap<String, PropertyValue>;

/// Type alias for a property reference (key-value pair)
pub type Property = (String, PropertyValue);

/// Trait for types that can be converted to PropertyValue
pub trait IntoPropertyValue {
    fn into_property_value(self) -> PropertyValue;
}

impl IntoPropertyValue for String {
    fn into_property_value(self) -> PropertyValue {
        PropertyValue::String(self)
    }
}

impl<'a> IntoPropertyValue for &'a str {
    fn into_property_value(self) -> PropertyValue {
        PropertyValue::String(self.to_string())
    }
}

impl IntoPropertyValue for i64 {
    fn into_property_value(self) -> PropertyValue {
        PropertyValue::Int64(self)
    }
}

impl IntoPropertyValue for f64 {
    fn into_property_value(self) -> PropertyValue {
        PropertyValue::Float64(self)
    }
}

impl IntoPropertyValue for bool {
    fn into_property_value(self) -> PropertyValue {
        PropertyValue::Bool(self)
    }
}

impl IntoPropertyValue for Vec<PropertyValue> {
    fn into_property_value(self) -> PropertyValue {
        PropertyValue::Vec(self)
    }
}

/// Utility functions for property creation
pub mod props {
    use super::*;
    
    /// Create a string property
    pub fn string(value: impl Into<String>) -> PropertyValue {
        PropertyValue::String(value.into())
    }
    
    /// Create an integer property
    pub fn int64(value: i64) -> PropertyValue {
        PropertyValue::Int64(value)
    }
    
    /// Create a float property
    pub fn float64(value: f64) -> PropertyValue {
        PropertyValue::Float64(value)
    }
    
    /// Create a boolean property
    pub fn bool(value: bool) -> PropertyValue {
        PropertyValue::Bool(value)
    }
    
    /// Create a null property
    pub fn null() -> PropertyValue {
        PropertyValue::Null
    }
    
    /// Create a vector property
    pub fn vec(values: Vec<PropertyValue>) -> PropertyValue {
        PropertyValue::Vec(values)
    }
    
    /// Create a properties HashMap from key-value pairs
    pub fn map(pairs: Vec<(impl Into<String>, impl IntoPropertyValue)>) -> Properties {
        pairs.into_iter()
            .map(|(k, v)| (k.into(), v.into_property_value()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::props::*;

    #[test]
    fn test_vertex_id_creation() {
        let id = VertexId::new(42);
        assert_eq!(id.value(), 42);
        assert_eq!(id.to_string(), "42");
    }

    #[test]
    fn test_vertex_id_conversions() {
        let id = VertexId::from(42u64);
        assert_eq!(u64::from(id), 42);
        
        let id_str: VertexId = "123".parse().unwrap();
        assert_eq!(id_str.value(), 123);
    }

    #[test]
    fn test_edge_creation() {
        let src = VertexId::new(1);
        let dst = VertexId::new(2);
        let edge = Edge::new(src, dst, "friend");
        
        assert_eq!(edge.src, src);
        assert_eq!(edge.dst, dst);
        assert_eq!(edge.label, "friend");
    }

    #[test]
    fn test_edge_from_ids() {
        let edge = Edge::from_ids(1, 2, "friend");
        assert_eq!(edge.src, VertexId::new(1));
        assert_eq!(edge.dst, VertexId::new(2));
        assert_eq!(edge.label, "friend");
    }

    #[test]
    fn test_edge_reversed() {
        let edge = Edge::from_ids(1, 2, "friend");
        let reversed = edge.reversed();
        
        assert_eq!(reversed.src, VertexId::new(2));
        assert_eq!(reversed.dst, VertexId::new(1));
        assert_eq!(reversed.label, "friend");
    }

    #[test]
    fn test_property_value_creation() {
        let string_prop = PropertyValue::string("hello");
        assert_eq!(string_prop.as_string(), Some("hello"));
        
        let int_prop = PropertyValue::int64(42);
        assert_eq!(int_prop.as_int64(), Some(42));
        
        let bool_prop = PropertyValue::bool(true);
        assert_eq!(bool_prop.as_bool(), Some(true));
        
        let null_prop = PropertyValue::Null;
        assert!(null_prop.is_null());
    }

    #[test]
    fn test_property_value_conversions() {
        // Test float access from int
        let int_prop = PropertyValue::int64(42);
        assert_eq!(int_prop.as_float64(), Some(42.0));
        
        // Test vector property
        let vec_prop = PropertyValue::vec(vec![
            PropertyValue::string("a"),
            PropertyValue::int64(1),
        ]);
        assert!(vec_prop.as_vec().is_some());
    }

    #[test]
    fn test_property_hash() {
        use std::collections::hash_map::DefaultHasher;
        
        let prop1 = PropertyValue::string("test");
        let prop2 = PropertyValue::string("test");
        
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        
        prop1.hash(&mut hasher1);
        prop2.hash(&mut hasher2);
        
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_props_utility() {
        // Test props::map with same-type values
        let string_props = props::map(vec![
            ("name", "Alice"),
            ("city", "NYC"),
        ]);
        assert_eq!(string_props.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(string_props.get("city").unwrap().as_string(), Some("NYC"));

        // Test with manually constructed mixed-type properties
        let mut props = HashMap::new();
        props.insert("name".to_string(), PropertyValue::string("Alice"));
        props.insert("age".to_string(), PropertyValue::int64(30));
        props.insert("active".to_string(), PropertyValue::bool(true));
        props.insert("score".to_string(), PropertyValue::float64(95.5));
        props.insert("tags".to_string(), PropertyValue::vec(vec![string("engineer"), string("rust")]));

        assert_eq!(props.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(props.get("age").unwrap().as_int64(), Some(30));
        assert_eq!(props.get("active").unwrap().as_bool(), Some(true));
        assert_eq!(props.get("score").unwrap().as_float64(), Some(95.5));
        assert!(props.get("tags").unwrap().as_vec().is_some());
    }

    #[test]
    fn test_property_display() {
        let string_prop = PropertyValue::string("hello");
        assert_eq!(string_prop.to_string(), "\"hello\"");
        
        let int_prop = PropertyValue::int64(42);
        assert_eq!(int_prop.to_string(), "42");
        
        let vec_prop = PropertyValue::vec(vec![
            PropertyValue::string("a"),
            PropertyValue::int64(1),
        ]);
        assert_eq!(vec_prop.to_string(), "[\"a\", 1]");
        
        let null_prop = PropertyValue::Null;
        assert_eq!(null_prop.to_string(), "null");
    }

    #[test]
    fn test_into_property_value() {
        let string_val: PropertyValue = "hello".into_property_value();
        assert_eq!(string_val.as_string(), Some("hello"));
        
        let int_val: PropertyValue = 42i64.into_property_value();
        assert_eq!(int_val.as_int64(), Some(42));
        
        let bool_val: PropertyValue = true.into_property_value();
        assert_eq!(bool_val.as_bool(), Some(true));
    }

    #[test]
    fn test_property_type_names() {
        assert_eq!(PropertyValue::string("test").type_name(), "string");
        assert_eq!(PropertyValue::int64(42).type_name(), "int64");
        assert_eq!(PropertyValue::float64(3.14).type_name(), "float64");
        assert_eq!(PropertyValue::bool(true).type_name(), "bool");
        assert_eq!(PropertyValue::vec(vec![]).type_name(), "vec");
        assert_eq!(PropertyValue::Null.type_name(), "null");
    }
}