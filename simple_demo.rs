//! Simplified Graph Database Demo
//! 
//! This demo showcases core functionality without requiring full compilation

use std::collections::HashMap;

// Simplified types for demo
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct VertexId(u64);

#[derive(Debug, Clone)]
struct Edge {
    src: VertexId,
    dst: VertexId,
    label: String,
}

impl VertexId {
    fn new(id: u64) -> Self { VertexId(id) }
}

impl Edge {
    fn new(src: VertexId, dst: VertexId, label: &str) -> Self {
        Edge {
            src, dst, label: label.to_string()
        }
    }
}

#[derive(Debug, Clone)]
enum PropertyValue {
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}

impl PropertyValue {
    fn string(s: &str) -> Self { PropertyValue::String(s.to_string()) }
    fn int64(i: i64) -> Self { PropertyValue::Int64(i) }
    fn float64(f: f64) -> Self { PropertyValue::Float64(f) }
    fn bool(b: bool) -> Self { PropertyValue::Bool(b) }
}

type Properties = HashMap<String, PropertyValue>;

fn main() {
    println!("🚀 Graph Database Demo - Simplified Version");
    println!("==========================================");
    
    // Create sample vertices
    let mut vertices = Properties::new();
    vertices.insert("Alice".to_string(), PropertyValue::int64(30));
    vertices.insert("Bob".to_string(), PropertyValue::int64(25));
    vertices.insert("Charlie".to_string(), PropertyValue::int64(35));
    
    // Create sample edges
    let edges = vec![
        Edge::new(VertexId::new(1), VertexId::new(2), "friend"),
        Edge::new(VertexId::new(1), VertexId::new(3), "colleague"),
        Edge::new(VertexId::new(2), VertexId::new(3), "friend"),
    ];
    
    println!("\n📊 Sample Graph Data:");
    println!("Vertices: {:?}", vertices);
    println!("Edges: {:?}", edges);
    
    // Demonstrate graph operations
    println!("\n🔍 Graph Operations:");
    
    // Count vertices and edges
    println!("Total vertices: {}", vertices.len());
    println!("Total edges: {}", edges.len());
    
    // Find edges involving vertex 1
    let vertex_1_edges: Vec<_> = edges.iter()
        .filter(|e| e.src.0 == 1 || e.dst.0 == 1)
        .collect();
    println!("Edges involving vertex 1: {}", vertex_1_edges.len());
    
    // Demonstrate different edge types
    let mut edge_types = std::collections::HashSet::new();
    for edge in &edges {
        edge_types.insert(&edge.label);
    }
    println!("Edge types: {:?}", edge_types);
    
    // Show graph connectivity
    println!("\n🔗 Connectivity Analysis:");
    for edge in &edges {
        println!("{} -> {} ({})", edge.src.0, edge.dst.0, edge.label);
    }
    
    // Simple degree calculation
    let mut degrees = HashMap::new();
    for edge in &edges {
        *degrees.entry(edge.src.0).or_insert(0) += 1;
        *degrees.entry(edge.dst.0).or_insert(0) += 1;
    }
    
    println!("\n📈 Vertex Degrees:");
    for (vertex, degree) in &degrees {
        println!("Vertex {}: degree {}", vertex, degree);
    }
    
    // Property examples
    println!("\n🏷️  Property Examples:");
    let person_props = vec![
        ("name", PropertyValue::string("Alice")),
        ("age", PropertyValue::int64(30)),
        ("active", PropertyValue::bool(true)),
        ("score", PropertyValue::float64(95.5)),
    ];
    
    for (key, value) in &person_props {
        match value {
            PropertyValue::String(s) => println!("  {}: \"{}\"", key, s),
            PropertyValue::Int64(i) => println!("  {}: {}", key, i),
            PropertyValue::Float64(f) => println!("  {}: {:.1}", key, f),
            PropertyValue::Bool(b) => println!("  {}: {}", key, b),
        }
    }
    
    println!("\n✅ Demo completed successfully!");
    println!("🎯 This demonstrates the core concepts that will be available in the full graph database:");
    println!("   • Vertex and Edge management");
    println!("   • Property storage");
    println!("   • Graph traversal operations");
    println!("   • Connectivity analysis");
    println!("   • Degree calculations");
    
    println!("\n🔧 To run the full demo with all features:");
    println!("   cargo run --bin graph_demo --features full");
}