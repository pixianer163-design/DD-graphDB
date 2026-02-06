use graph_core::*;
use std::collections::HashMap;

#[cfg(feature = "streaming")]
use differential_dataflow::collection::Collection;
#[cfg(feature = "streaming")]
use timely::dataflow::Scope;

/// A graph structure using differential dataflow collections for incremental processing
#[cfg(feature = "streaming")]
pub struct GraphCollection<G: Scope> {
    /// Collection of vertices with their properties (VertexId, Properties)
    pub vertices: Collection<G, (VertexId, HashMap<String, PropertyValue>)>,
    /// Collection of edges with their properties (Edge, Properties)  
    pub edges: Collection<G, (Edge, HashMap<String, PropertyValue>)>,
}

#[cfg(feature = "streaming")]
impl<G: Scope> GraphCollection<G>
where
    G::Timestamp: differential_dataflow::lattice::Lattice + Ord,
{
    /// Create a new GraphCollection from vertex and edge collections
    pub fn new(
        vertices: Collection<G, (VertexId, HashMap<String, PropertyValue>)>,
        edges: Collection<G, (Edge, HashMap<String, PropertyValue>)>,
    ) -> Self {
        Self { vertices, edges }
    }

    /// Create an empty GraphCollection
    pub fn empty(scope: &mut G) -> Self {
        Self {
            vertices: Collection::new(scope),
            edges: Collection::new(scope),
        }
    }

#[cfg(feature = "streaming")]
#[cfg(feature = "streaming")]
    /// Get all vertices that have a specific label
    pub fn vertices_with_label(&self, label: &str) -> Collection<G, VertexId> {
        self.vertices
            .filter(move |(_, props)| {
                props.get("type")
                    .and_then(|p| p.as_string())
                    .map(|l| l == label)
                    .unwrap_or(false)
            })
            .map(|(v, _)| v)
    }

#[cfg(feature = "streaming")]
    /// Get all edges with a specific label
    pub fn edges_with_label(&self, label: &str) -> Collection<G, Edge> {
        self.edges
            .filter(move |(edge, _)| edge.label == label)
            .map(|(edge, _)| edge)
    }

#[cfg(feature = "streaming")]
    /// Get all neighbors of a vertex (both incoming and outgoing)
    pub fn neighbors(&self, vertex: VertexId) -> Collection<G, VertexId> {
        // Create a collection with just the target vertex for joining
        let target = self.vertices
            .filter(move |(v, _)| *v == vertex)
            .map(|(v, _)| (v, ()));

        // Outgoing neighbors
        let outgoing = self.edges
            .map(|(edge, _)| (edge.src, edge.dst))
            .join_core(&target, |_src, dst, _| Some(*dst));

        // Incoming neighbors
        let incoming = self.edges
            .map(|(edge, _)| (edge.dst, edge.src))
            .join_core(&target, |_dst, src, _| Some(*src));

        outgoing.concat(&incoming).distinct()
    }

#[cfg(feature = "streaming")]
    /// Get outgoing neighbors of a vertex
    pub fn outgoing_neighbors(&self, vertex: VertexId) -> Collection<G, VertexId> {
        let target = self.vertices
            .filter(move |(v, _)| *v == vertex)
            .map(|(v, _)| (v, ()));

        self.edges
            .map(|(edge, _)| (edge.src, edge.dst))
            .join_core(&target, |_src, dst, _| Some(*dst))
            .distinct()
    }

#[cfg(feature = "streaming")]
    /// Get incoming neighbors of a vertex
    pub fn incoming_neighbors(&self, vertex: VertexId) -> Collection<G, VertexId> {
        let target = self.vertices
            .filter(move |(v, _)| *v == vertex)
            .map(|(v, _)| (v, ()));

        self.edges
            .map(|(edge, _)| (edge.dst, edge.src))
            .join_core(&target, |_dst, src, _| Some(*src))
            .distinct()
    }

#[cfg(feature = "streaming")]
    /// Get out-degree of a vertex
    pub fn out_degree(&self, vertex: VertexId) -> Collection<G, (VertexId, usize)> {
        let target = self.vertices
            .filter(move |(v, _)| *v == vertex)
            .map(|(v, _)| (v, ()));

        self.edges
            .map(|(edge, _)| (edge.src, ()))
            .join_core(&target, |src, (), _| Some((*src, 1)))
            .count()
    }

#[cfg(feature = "streaming")]
    /// Get in-degree of a vertex
    pub fn in_degree(&self, vertex: VertexId) -> Collection<G, (VertexId, usize)> {
        let target = self.vertices
            .filter(move |(v, _)| *v == vertex)
            .map(|(v, _)| (v, ()));

        self.edges
            .map(|(edge, _)| (edge.dst, ()))
            .join_core(&target, |dst, (), _| Some((*dst, 1)))
            .count()
    }

#[cfg(feature = "streaming")]
    /// Compute degree distribution of the entire graph
    pub fn degree_distribution(&self) -> Collection<G, (usize, usize)> {
        // Count out-degrees for all vertices
        self.edges
            .map(|(edge, _)| edge.src)
            .count()
            .map(|(_v, deg)| deg as usize)
            .count()
    }

#[cfg(feature = "streaming")]
    /// Get edges between two vertices
    pub fn edges_between(&self, src: VertexId, dst: VertexId) -> Collection<G, Edge> {
        self.edges
            .filter(move |(edge, _)| {
                (edge.src == src && edge.dst == dst) || 
                (edge.src == dst && edge.dst == src)
            })
            .map(|(edge, _)| edge)
    }

#[cfg(feature = "streaming")]
    /// Filter vertices by property conditions
    pub fn filter_vertices<F>(&self, predicate: F) -> Collection<G, VertexId>
    where
        F: Fn(&VertexId, &HashMap<String, PropertyValue>) -> bool + 'static,
    {
        self.vertices
            .filter(predicate)
            .map(|(v, _)| v)
    }

#[cfg(feature = "streaming")]
    /// Filter edges by property conditions
    pub fn filter_edges<F>(&self, predicate: F) -> Collection<G, Edge>
    where
        F: Fn(&Edge, &HashMap<String, PropertyValue>) -> bool + 'static,
    {
        self.edges
            .filter(predicate)
            .map(|(edge, _)| edge)
    }

#[cfg(feature = "streaming")]
    /// Get vertex properties for a specific vertex
    pub fn vertex_properties(&self, vertex: VertexId) -> Collection<G, HashMap<String, PropertyValue>> {
        self.vertices
            .filter(move |(v, _)| *v == vertex)
            .map(|(_, props)| props)
    }

#[cfg(feature = "streaming")]
    /// Count total number of vertices
    pub fn vertex_count(&self) -> Collection<G, usize> {
        self.vertices.count().map(|(_, count)| count as usize)
    }

#[cfg(feature = "streaming")]
    /// Count total number of edges
    pub fn edge_count(&self) -> Collection<G, usize> {
        self.edges.count().map(|(_, count)| count as usize)
    }
}

/// A simple graph collection for offline development without streaming dependencies
#[cfg(not(feature = "streaming"))]
pub struct SimpleGraphCollection {
    /// Simple vector of vertices with their properties
    pub vertices: Vec<(VertexId, HashMap<String, PropertyValue>)>,
    /// Simple vector of edges with their properties  
    pub edges: Vec<(Edge, HashMap<String, PropertyValue>)>,
}

#[cfg(not(feature = "streaming"))]
impl SimpleGraphCollection {
    /// Create a new SimpleGraphCollection from vertex and edge vectors
    pub fn new(
        vertices: Vec<(VertexId, HashMap<String, PropertyValue>)>,
        edges: Vec<(Edge, HashMap<String, PropertyValue>)>,
    ) -> Self {
        Self { vertices, edges }
    }

    /// Create an empty SimpleGraphCollection
    pub fn empty() -> Self {
        Self {
            vertices: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Get all vertices that have a specific label
    pub fn vertices_with_label(&self, label: &str) -> Vec<VertexId> {
        self.vertices
            .iter()
            .filter(|(_, props)| {
                props.get("type")
                    .and_then(|p| p.as_string())
                    .map(|l| l == label)
                    .unwrap_or(false)
            })
            .map(|(v, _)| *v)
            .collect()
    }

    /// Get all edges with a specific label
    pub fn edges_with_label(&self, label: &str) -> Vec<Edge> {
        self.edges
            .iter()
            .filter(|(edge, _)| edge.label == label)
            .map(|(edge, _)| edge.clone())
            .collect()
    }

    /// Get all neighbors of a vertex (both incoming and outgoing)
    pub fn neighbors(&self, vertex: VertexId) -> Vec<VertexId> {
        let mut neighbors = std::collections::HashSet::new();
        
        for (edge, _) in &self.edges {
            if edge.src == vertex {
                neighbors.insert(edge.dst);
            }
            if edge.dst == vertex {
                neighbors.insert(edge.src);
            }
        }
        
        neighbors.into_iter().collect()
    }

    /// Get outgoing neighbors of a vertex
    pub fn outgoing_neighbors(&self, vertex: VertexId) -> Vec<VertexId> {
        self.edges
            .iter()
            .filter(|(edge, _)| edge.src == vertex)
            .map(|(edge, _)| edge.dst)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get incoming neighbors of a vertex
    pub fn incoming_neighbors(&self, vertex: VertexId) -> Vec<VertexId> {
        self.edges
            .iter()
            .filter(|(edge, _)| edge.dst == vertex)
            .map(|(edge, _)| edge.src)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get out-degree of a vertex
    pub fn out_degree(&self, vertex: VertexId) -> usize {
        self.edges
            .iter()
            .filter(|(edge, _)| edge.src == vertex)
            .count()
    }

    /// Get in-degree of a vertex
    pub fn in_degree(&self, vertex: VertexId) -> usize {
        self.edges
            .iter()
            .filter(|(edge, _)| edge.dst == vertex)
            .count()
    }

    /// Compute degree distribution of the entire graph
    pub fn degree_distribution(&self) -> Vec<(usize, usize)> {
        let mut degree_counts = std::collections::HashMap::new();
        
        // Count degrees for all vertices
        for (vertex, _) in &self.vertices {
            let degree = self.out_degree(*vertex) + self.in_degree(*vertex);
            *degree_counts.entry(degree).or_insert(0) += 1;
        }
        
        degree_counts.into_iter().collect()
    }

    /// Get edges between two vertices
    pub fn edges_between(&self, src: VertexId, dst: VertexId) -> Vec<Edge> {
        self.edges
            .iter()
            .filter(|(edge, _)| {
                (edge.src == src && edge.dst == dst) || 
                (edge.src == dst && edge.dst == src)
            })
            .map(|(edge, _)| edge.clone())
            .collect()
    }

    /// Filter vertices by property conditions
    pub fn filter_vertices<F>(&self, predicate: F) -> Vec<VertexId>
    where
        F: Fn(&VertexId, &HashMap<String, PropertyValue>) -> bool,
    {
        self.vertices
            .iter()
            .filter(|(v, props)| predicate(v, props))
            .map(|(v, _)| *v)
            .collect()
    }

    /// Filter edges by property conditions
    pub fn filter_edges<F>(&self, predicate: F) -> Vec<Edge>
    where
        F: Fn(&Edge, &HashMap<String, PropertyValue>) -> bool,
    {
        self.edges
            .iter()
            .filter(|(edge, props)| predicate(edge, props))
            .map(|(edge, _)| edge.clone())
            .collect()
    }

    /// Get vertex properties for a specific vertex
    pub fn vertex_properties(&self, vertex: VertexId) -> Option<HashMap<String, PropertyValue>> {
        self.vertices
            .iter()
            .find(|(v, _)| *v == vertex)
            .map(|(_, props)| props.clone())
    }

    /// Count total number of vertices
    pub fn vertex_count(&self) -> usize {
        self.vertices.len()
    }

    /// Count total number of edges
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_core::props;

    #[test]
    fn test_edge_operations() {
        // Test edge creation logic
        let edge1 = Edge::from_ids(1, 2, "friend");
        let edge2 = Edge::new(VertexId::new(1), VertexId::new(3), "colleague");
        
        assert_eq!(edge1.src, VertexId::new(1));
        assert_eq!(edge1.dst, VertexId::new(2));
        assert_eq!(edge1.label, "friend");
        
        assert_eq!(edge2.src, VertexId::new(1));
        assert_eq!(edge2.dst, VertexId::new(3));
        assert_eq!(edge2.label, "colleague");
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
    fn test_edge_connects_same_vertices() {
        let edge1 = Edge::from_ids(1, 2, "friend");
        let edge2 = Edge::from_ids(1, 2, "colleague");
        let edge3 = Edge::from_ids(2, 1, "friend_reverse");
        
        assert!(edge1.connects_same_vertices(&edge2));
        assert!(edge1.connects_same_vertices(&edge3));
        assert!(!edge2.connects_same_vertices(&Edge::from_ids(3, 4, "other")));
    }

    #[test]
    fn test_property_creation() {
        use std::collections::HashMap;

        // Test with same-type values using props::map
        let string_props = props::map(vec![
            ("name", "Alice"),
            ("city", "NYC"),
        ]);
        assert_eq!(string_props.get("name").unwrap().as_string(), Some("Alice"));

        // Test with mixed types using manual HashMap construction
        let mut props = HashMap::new();
        props.insert("name".to_string(), PropertyValue::string("Alice"));
        props.insert("age".to_string(), PropertyValue::int64(30));
        props.insert("active".to_string(), PropertyValue::bool(true));
        props.insert("score".to_string(), PropertyValue::float64(95.5));

        assert_eq!(props.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(props.get("age").unwrap().as_int64(), Some(30));
        assert_eq!(props.get("active").unwrap().as_bool(), Some(true));
        assert_eq!(props.get("score").unwrap().as_float64(), Some(95.5));
    }
}