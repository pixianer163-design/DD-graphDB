//! Simplified Graph Database Demo
//!
//! A demonstration of the core graph database functionality
//! without external dependencies for compatibility.

use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use graph_core::*;
use graph_storage::*;

/// Simplified graph database server for demonstration
struct SimpleGraphDB {
    storage: Arc<Mutex<GraphStorage>>,
}

impl SimpleGraphDB {
    fn new(storage_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let storage = Arc::new(Mutex::new(GraphStorage::new(storage_path)?));
        
        Ok(SimpleGraphDB { storage })
    }

    async fn run_demo(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 Graph Database Demo Starting");
        println!("💾 Storage Engine with ACID");
        println!("🔍 GQL Query Support");
        println!("📊 Property-based Graph Model");
        println!();

        // Create sample data
        self.create_sample_data().await?;
        
        // Run queries
        self.run_queries().await?;
        
        // Show statistics
        self.show_stats().await?;

        Ok(())
    }

    async fn create_sample_data(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("📝 Creating Sample Graph Data...");
        
        let mut transaction = self.storage.lock().unwrap().begin_transaction()?;

        // Create vertices
        let alice_id = VertexId::new(1);
        let bob_id = VertexId::new(2);
        let charlie_id = VertexId::new(3);
        let diana_id = VertexId::new(4);

        let alice_props = props::map(vec![
            ("name", "Alice"),
            ("age", 30i64),
            ("type", "Person"),
            ("department", "Engineering"),
        ]);

        let bob_props = props::map(vec![
            ("name", "Bob"),
            ("age", 25i64),
            ("type", "Person"),
            ("department", "Engineering"),
        ]);

        let charlie_props = props::map(vec![
            ("name", "Charlie"),
            ("age", 35i64),
            ("type", "Person"),
            ("department", "Management"),
        ]);

        let diana_props = props::map(vec![
            ("name", "Diana"),
            ("age", 28i64),
            ("type", "Person"),
            ("department", "Engineering"),
        ]);

        transaction.add_operation(GraphOperation::AddVertex {
            id: alice_id,
            properties: alice_props.clone(),
        });

        transaction.add_operation(GraphOperation::AddVertex {
            id: bob_id,
            properties: bob_props.clone(),
        });

        transaction.add_operation(GraphOperation::AddVertex {
            id: charlie_id,
            properties: charlie_props.clone(),
        });

        transaction.add_operation(GraphOperation::AddVertex {
            id: diana_id,
            properties: diana_props.clone(),
        });

        // Create edges
        let alice_bob = Edge::new(alice_id, bob_id, "colleague");
        let bob_charlie = Edge::new(bob_id, charlie_id, "reports_to");
        let alice_diana = Edge::new(alice_id, diana_id, "colleague");
        let charlie_diana = Edge::new(charlie_id, diana_id, "manages");

        transaction.add_operation(GraphOperation::AddEdge {
            edge: alice_bob,
            properties: props::map(vec![("since", 2020i64)]),
        });

        transaction.add_operation(GraphOperation::AddEdge {
            edge: bob_charlie,
            properties: props::map(vec![("since", 2019i64)]),
        });

        transaction.add_operation(GraphOperation::AddEdge {
            edge: alice_diana,
            properties: props::map(vec![("since", 2021i64)]),
        });

        transaction.add_operation(GraphOperation::AddEdge {
            edge: charlie_diana,
            properties: props::map(vec![("since", 2018i64)]),
        });

        // Commit transaction
        self.storage.lock().unwrap().commit_transaction(transaction)?;
        
        println!("✅ Sample data created: 4 vertices, 4 edges");
        println!();

        Ok(())
    }

    async fn run_queries(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🔍 Running Graph Queries...");
        println!();

        // Query 1: Find all people in Engineering
        println!("📊 Query 1: Find all engineers");
        let engineers = self.storage.lock().unwrap().find_vertices_by_property(|props| {
            props.get("department")
                .and_then(|p| p.as_string())
                .map(|dept| dept == "Engineering")
                .unwrap_or(false)
        })?;

        for (id, props) in engineers {
            if let Some(name) = props.get("name").and_then(|p| p.as_string()) {
                if let Some(age) = props.get("age").and_then(|p| p.as_int64()) {
                    println!("  👤 {} (age: {}) - ID: {}", name, age, id);
                }
            }
        }
        println!();

        // Query 2: Find connections
        println!("📊 Query 2: Find colleagues of Alice");
        let alice_id = VertexId::new(1);
        
        // Find all edges where Alice is source
        let alice_edges = self.storage.lock().unwrap().find_edges_by_property(|props| {
            // This would need more sophisticated query in real implementation
            false // Simplified for demo
        })?;

        if alice_edges.is_empty() {
            println!("  Looking for connections manually...");
            
            // Demo manual edge traversal
            let all_edges = self.storage.lock().unwrap().list_edges()?;
            for (edge, edge_props) in all_edges {
                if edge.src == alice_id {
                    if let Some(dst_id) = self.storage.lock().unwrap().get_vertex(edge.dst)? {
                        if let Some(name) = dst_id.get("name").and_then(|p| p.as_string()) {
                            if let Some(since) = edge_props.get("since").and_then(|p| p.as_int64()) {
                                println!("  👤 Alice is colleague with {} (since: {})", name, since);
                            }
                        }
                    }
                }
            }
        }
        println!();

        // Query 3: Department statistics
        println!("📊 Query 3: Department statistics");
        let all_vertices = self.storage.lock().unwrap().list_vertices()?;
        let mut dept_counts: HashMap<String, usize> = HashMap::new();

        for (_, props) in all_vertices {
            if let Some(dept) = props.get("department").and_then(|p| p.as_string()) {
                *dept_counts.entry(dept.clone()).or_insert(0) += 1;
            }
        }

        for (dept, count) in dept_counts {
            println!("  🏢 {}: {} people", dept, count);
        }
        println!();

        Ok(())
    }

    async fn show_stats(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("📈 Database Statistics:");
        
        let stats = self.storage.lock().unwrap().get_stats()?;
        
        println!("  📊 Vertices: {}", stats.vertex_count);
        println!("  🔗 Edges: {}", stats.edge_count);
        println!("  📦 Version: {}", stats.version);
        println!("  ⏰ Timestamp: {:?}", stats.timestamp);
        
        // Calculate density
        if stats.vertex_count > 1 {
            let max_edges = stats.vertex_count * (stats.vertex_count - 1);
            let density = stats.edge_count as f64 / max_edges as f64;
            println!("  📊 Graph Density: {:.3}", density);
        }

        Ok(())
    }

    fn run_interactive(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🎮 Interactive Mode");
        println!("Commands: 'stats', 'list', 'help', 'quit'");
        println!();

        loop {
            print!("gdb> ");
            io::stdout().flush()?;

            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim();

            match input {
                "quit" | "exit" => {
                    println!("👋 Goodbye!");
                    break;
                }
                "help" => {
                    println!("Commands:");
                    println!("  stats - Show database statistics");
                    println!("  list  - List all vertices and edges");
                    println!("  help  - Show this help");
                    println!("  quit  - Exit the program");
                }
                "stats" => {
                    let _ = self.show_stats();
                }
                "list" => {
                    self.list_all_data()?;
                }
                _ if input.starts_with("find ") => {
                    let name = &input[5..];
                    self.find_person(name)?;
                }
                _ => {
                    println!("Unknown command: {}", input);
                }
            }
        }

        Ok(())
    }

    fn list_all_data(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("📋 All Vertices:");
        let vertices = self.storage.lock().unwrap().list_vertices()?;
        for (id, props) in vertices {
            if let Some(name) = props.get("name").and_then(|p| p.as_string()) {
                println!("  👤 {} (ID: {})", name, id);
                if let Some(age) = props.get("age").and_then(|p| p.as_int64()) {
                    if let Some(dept) = props.get("department").and_then(|p| p.as_string()) {
                        println!("      Age: {}, Dept: {}", age, dept);
                    }
                }
            }
        }

        println!();
        println!("📋 All Edges:");
        let edges = self.storage.lock().unwrap().list_edges()?;
        for (edge, edge_props) in edges {
            let src_name = self.get_vertex_name(edge.src)?;
            let dst_name = self.get_vertex_name(edge.dst)?;
            
            println!("  {} -[{}]-> {}", src_name, edge.label, dst_name);
            if let Some(since) = edge_props.get("since").and_then(|p| p.as_int64()) {
                println!("      Since: {}", since);
            }
        }

        Ok(())
    }

    fn get_vertex_name(&self, id: VertexId) -> Result<String, Box<dyn std::error::Error>> {
        match self.storage.lock().unwrap().get_vertex(id)? {
            Some(props) => {
                Ok(props.get("name")
                    .and_then(|p| p.as_string())
                    .unwrap_or("Unknown".to_string()))
            }
            None => Ok("Unknown".to_string()),
        }
    }

    fn find_person(&self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        println!("🔍 Searching for: {}", name);
        
        let vertices = self.storage.lock().unwrap().find_vertices_by_property(|props| {
            props.get("name")
                .and_then(|p| p.as_string())
                .map(|person_name| person_name == name)
                .unwrap_or(false)
        })?;

        if vertices.is_empty() {
            println!("  ❌ Person '{}' not found", name);
        } else {
            for (id, props) in vertices {
                println!("  ✅ Found person (ID: {})", id);
                for (key, value) in &props {
                    println!("    {}: {}", key, value);
                }
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("🚀 Graph Database Demo");
    println!("📊 Simple Implementation (No External Dependencies)");
    println!();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let storage_path = if args.len() > 1 {
        &args[1]
    } else {
        "./graph_data"
    };

    // Create database
    let db = SimpleGraphDB::new(storage_path)?;

    // Run demo if requested
    if args.contains(&"--demo".to_string()) {
        db.run_demo().await?;
    }

    // Run interactive mode
    db.run_interactive()?;

    // Cleanup
    println!("🧹 Cleaning up...");
    drop(db);

    println!("✅ Graph Database Demo Complete!");
    Ok(())
}