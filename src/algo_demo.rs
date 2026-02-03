//! Graph Algorithms Demo
//!
//! Demonstrates PageRank, Connected Components, and Dijkstra algorithms

use std::collections::HashMap;
use std::sync::Arc;

use graph_core::{VertexId, Edge, props};
use graph_storage::{GraphStorage, GraphOperation};
use graph_algorithms::{
    compute_pagerank,
    find_connected_components,
    compute_shortest_path,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Graph Algorithms Demo");
    println!("=========================\n");
    
    // Create test graph
    let storage = create_test_graph()?;
    println!("✅ Test graph created\n");
    
    // Test 1: PageRank
    test_pagerank(&storage)?;
    
    // Test 2: Connected Components
    test_connected_components(&storage)?;
    
    // Test 3: Dijkstra Shortest Path
    test_dijkstra(&storage)?;
    
    println!("\n🎉 All algorithm tests completed!");
    
    Ok(())
}

fn create_test_graph() -> Result<Arc<GraphStorage>, Box<dyn std::error::Error>> {
    let temp_dir = std::env::temp_dir().join("algo_demo_db");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir)?;
    
    let storage = Arc::new(GraphStorage::new(&temp_dir)?);
    let mut transaction = storage.begin_transaction()?;
    
    // Create vertices (websites)
    let vertices = vec![
        (1, "Home", "hub"),
        (2, "About", "page"),
        (3, "Products", "hub"),
        (4, "Contact", "page"),
        (5, "Blog", "hub"),
        (6, "Article1", "content"),
        (7, "Article2", "content"),
    ];
    
    for (id, name, type_) in vertices {
        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(id),
            properties: props::map(vec![
                ("name", name),
                ("type", type_),
            ]),
        });
    }
    
    // Create edges (links between pages)
    // Home links to all major sections
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(1), VertexId::new(2), "links"),
        properties: HashMap::new(),
    });
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(1), VertexId::new(3), "links"),
        properties: HashMap::new(),
    });
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(1), VertexId::new(4), "links"),
        properties: HashMap::new(),
    });
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(1), VertexId::new(5), "links"),
        properties: HashMap::new(),
    });
    
    // Products links back to Home and Contact
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(3), VertexId::new(1), "links"),
        properties: HashMap::new(),
    });
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(3), VertexId::new(4), "links"),
        properties: HashMap::new(),
    });
    
    // Blog links to articles and Home
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(5), VertexId::new(6), "links"),
        properties: HashMap::new(),
    });
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(5), VertexId::new(7), "links"),
        properties: HashMap::new(),
    });
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(5), VertexId::new(1), "links"),
        properties: HashMap::new(),
    });
    
    // Articles link back to Blog
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(6), VertexId::new(5), "links"),
        properties: HashMap::new(),
    });
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(7), VertexId::new(5), "links"),
        properties: HashMap::new(),
    });
    
    // About links to Contact
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(2), VertexId::new(4), "links"),
        properties: HashMap::new(),
    });
    
    storage.commit_transaction(transaction)?;
    
    Ok(storage)
}

fn test_pagerank(storage: &GraphStorage) -> Result<(), Box<dyn std::error::Error>> {
    println!("📍 Test 1: PageRank Algorithm");
    println!("   Computing importance scores for web pages...");
    
    let result = compute_pagerank(storage, 0.85, 100, 0.0001)?;
    
    println!("   ✅ Converged after {} iterations", result.iterations);
    println!("   📊 Convergence delta: {:.6}", result.convergence_delta);
    println!("\n   Top 5 Pages by PageRank:");
    
    for (i, (vertex_id, score)) in result.top_n(5).iter().enumerate() {
        if let Ok(Some(props)) = storage.get_vertex(*vertex_id) {
            let name = props.get("name").and_then(|p| p.as_string()).unwrap_or("Unknown");
            println!("   {}. {} (ID: {}): {:.4}", i + 1, name, vertex_id.0, score);
        }
    }
    
    println!();
    Ok(())
}

fn test_connected_components(storage: &GraphStorage) -> Result<(), Box<dyn std::error::Error>> {
    println!("📍 Test 2: Connected Components");
    println!("   Finding connected subgraphs...");
    
    let result = find_connected_components(storage)?;
    
    println!("   ✅ Found {} connected component(s)", result.num_components);
    
    // Group vertices by component
    let mut components_map: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
    for (vertex, component) in &result.components {
        components_map.entry(*component).or_default().push(*vertex);
    }
    
    println!("\n   Component Details:");
    for (i, (component_id, vertices)) in components_map.iter().enumerate() {
        print!("   Component {} (ID: {}): ", i + 1, component_id.0);
        let names: Vec<String> = vertices.iter()
            .filter_map(|v| storage.get_vertex(*v).ok().flatten())
            .filter_map(|p| p.get("name").and_then(|n| n.as_string()).map(|s| s.to_string()))
            .collect();
        println!("{}", names.join(", "));
    }
    
    println!();
    Ok(())
}

fn test_dijkstra(storage: &GraphStorage) -> Result<(), Box<dyn std::error::Error>> {
    println!("📍 Test 3: Dijkstra Shortest Path");
    println!("   Finding shortest path from Home to Article2...");
    
    let start = VertexId::new(1); // Home
    let end = VertexId::new(7);   // Article2
    
    let result = compute_shortest_path(storage, start, Some(end), None)?;
    
    // Get path length before we potentially move the value
    let path_len = result.path.as_ref().map(|p| p.len());
    
    if let Some(ref path) = result.path {
        println!("   ✅ Path found! Length: {} hops", path_len.unwrap() - 1);
        
        print!("   Path: ");
        for (i, vertex_id) in path.iter().enumerate() {
            if let Ok(Some(props)) = storage.get_vertex(*vertex_id) {
                let name = props.get("name").and_then(|p| p.as_string()).unwrap_or("?");
                if i == 0 {
                    print!("{}", name);
                } else {
                    print!(" -> {}", name);
                }
            }
        }
        println!();
        
        if let Some(distance) = result.get_distance(end) {
            println!("   Distance: {} (unweighted)", distance);
        }
    } else {
        println!("   ❌ No path found");
    }
    
    println!();
    Ok(())
}
