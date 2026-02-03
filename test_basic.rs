#[cfg(test)]
mod basic_tests {
    use graph_core::{VertexId, Edge, PropertyValue, props};
    
    #[test]
    fn test_basic_types() {
        let id = VertexId::new(1);
        let edge = Edge::from_ids(1, 2, "test");
        let prop = PropertyValue::string("hello");
        
        assert_eq!(id.value(), 1);
        assert_eq!(edge.src, VertexId::new(1));
        assert_eq!(prop.as_string(), Some("hello"));
    }
    
    #[test]
    fn test_props_utility() {
        let props = props::map(vec![
            ("name", "Alice"),
            ("age", 30i64),
        ]);
        
        assert_eq!(props.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(props.get("age").unwrap().as_int64(), Some(30));
    }
}