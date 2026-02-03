//! Standalone Graph Database Demo
//!
//! A fully self-contained demonstration of the graph database functionality

use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

// Minimal data structures for demo
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct VertexId(u64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Edge {
    src: VertexId,
    dst: VertexId,
    label: String,
}

#[derive(Debug, Clone, PartialEq)]
enum PropertyValue {
    String(String),
    Int64(i64),
    Bool(bool),
    Float64(f64),
}

impl std::fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropertyValue::String(s) => write!(f, "\"{}\"", s),
            PropertyValue::Int64(i) => write!(f, "{}", i),
            PropertyValue::Bool(b) => write!(f, "{}", b),
            PropertyValue::Float64(fl) => write!(f, "{}", fl),
        }
    }
}

impl std::fmt::Display for VertexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

type Properties = HashMap<String, PropertyValue>;

/// Simple in-memory graph storage
struct SimpleStorage {
    vertices: Arc<Mutex<HashMap<VertexId, Properties>>>,
    edges: Arc<Mutex<HashMap<(VertexId, VertexId), (Edge, Properties)>>>,
    next_id: Arc<Mutex<u64>>,
}

impl SimpleStorage {
    fn new() -> Self {
        Self {
            vertices: Arc::new(Mutex::new(HashMap::new())),
            edges: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
        }
    }

    fn next_vertex_id(&self) -> VertexId {
        let mut next = self.next_id.lock().unwrap();
        let id = VertexId(*next);
        *next += 1;
        id
    }

    fn add_vertex(&self, id: VertexId, properties: Properties) {
        let mut vertices = self.vertices.lock().unwrap();
        vertices.insert(id, properties);
    }

    fn add_edge(&self, edge: Edge, properties: Properties) {
        let mut edges = self.edges.lock().unwrap();
        edges.insert((edge.src, edge.dst), (edge, properties));
    }

    fn get_vertex(&self, id: VertexId) -> Option<Properties> {
        let vertices = self.vertices.lock().unwrap();
        vertices.get(&id).cloned()
    }

    fn list_vertices(&self) -> Vec<(VertexId, Properties)> {
        let vertices = self.vertices.lock().unwrap();
        vertices.iter().map(|(id, props)| (*id, props.clone())).collect()
    }

    fn list_edges(&self) -> Vec<(Edge, Properties)> {
        let edges = self.edges.lock().unwrap();
        edges.values().map(|(edge, props)| (edge.clone(), props.clone())).collect()
    }

    fn find_vertices_by_property<F>(&self, predicate: F) -> Vec<(VertexId, Properties)>
    where
        F: Fn(&Properties) -> bool,
    {
        let vertices = self.vertices.lock().unwrap();
        vertices
            .iter()
            .filter(|(_, props)| predicate(props))
            .map(|(id, props)| (*id, props.clone()))
            .collect()
    }

    fn get_stats(&self) -> (usize, usize) {
        let vertices = self.vertices.lock().unwrap();
        let edges = self.edges.lock().unwrap();
        (vertices.len(), edges.len())
    }
}

/// Interactive graph database demo
struct GraphDemo {
    storage: Arc<SimpleStorage>,
}

impl GraphDemo {
    fn new() -> Self {
        Self {
            storage: Arc::new(SimpleStorage::new()),
        }
    }

    fn run_demo(&self) {
        println!("🚀 Graph Database Demo Starting");
        println!("💾 In-Memory Storage");
        println!("🔍 Property-based Queries");
        println!("📊 Real-time Statistics");
        println!();

        self.create_sample_data();
        self.show_stats();
        self.run_queries();
    }

    fn create_sample_data(&self) {
        println!("📝 Creating Sample Graph Data...");
        
        let alice_id = self.storage.next_vertex_id();
        let bob_id = self.storage.next_vertex_id();
        let charlie_id = self.storage.next_vertex_id();
        let diana_id = self.storage.next_vertex_id();
        let eve_id = self.storage.next_vertex_id();

        // Create vertices with properties
        let alice_props = self.create_person_props("Alice", 30, "Engineering", "Senior");
        let bob_props = self.create_person_props("Bob", 25, "Engineering", "Junior");
        let charlie_props = self.create_person_props("Charlie", 35, "Management", "Manager");
        let diana_props = self.create_person_props("Diana", 28, "Engineering", "Mid");
        let eve_props = self.create_person_props("Eve", 32, "Engineering", "Senior");

        self.storage.add_vertex(alice_id, alice_props);
        self.storage.add_vertex(bob_id, bob_props);
        self.storage.add_vertex(charlie_id, charlie_props);
        self.storage.add_vertex(diana_id, diana_props);
        self.storage.add_vertex(eve_id, eve_props);

        // Create edges with relationships
        let alice_bob = Edge { src: alice_id, dst: bob_id, label: "mentors".to_string() };
        let bob_charlie = Edge { src: bob_id, dst: charlie_id, label: "reports_to".to_string() };
        let charlie_diana = Edge { src: charlie_id, dst: diana_id, label: "manages".to_string() };
        let alice_diana = Edge { src: alice_id, dst: diana_id, label: "collaborates_with".to_string() };
        let diana_eve = Edge { src: diana_id, dst: eve_id, label: "mentors".to_string() };
        let alice_eve = Edge { src: alice_id, dst: eve_id, label: "colleague".to_string() };

        self.storage.add_edge(alice_bob, self.create_edge_props("2020"));
        self.storage.add_edge(bob_charlie, self.create_edge_props("2019"));
        self.storage.add_edge(charlie_diana, self.create_edge_props("2018"));
        self.storage.add_edge(alice_diana, self.create_edge_props("2021"));
        self.storage.add_edge(diana_eve, self.create_edge_props("2022"));
        self.storage.add_edge(alice_eve, self.create_edge_props("2020"));

        println!("✅ Sample data created: 5 vertices, 5 edges");
        println!();
    }

    fn create_person_props(&self, name: &str, age: i64, dept: &str, level: &str) -> Properties {
        let mut props = HashMap::new();
        props.insert("name".to_string(), PropertyValue::String(name.to_string()));
        props.insert("age".to_string(), PropertyValue::Int64(age));
        props.insert("department".to_string(), PropertyValue::String(dept.to_string()));
        props.insert("level".to_string(), PropertyValue::String(level.to_string()));
        props
    }

    fn create_edge_props(&self, since: &str) -> Properties {
        let mut props = HashMap::new();
        props.insert("since".to_string(), PropertyValue::Int64(since.parse().unwrap_or(2020)));
        props
    }

    fn show_stats(&self) {
        println!("📈 Database Statistics:");
        
        let (vertex_count, edge_count) = self.storage.get_stats();
        println!("  👥 Vertices: {}", vertex_count);
        println!("  🔗 Edges: {}", edge_count);
        
        // Calculate graph density
        if vertex_count > 1 {
            let max_edges = vertex_count * (vertex_count - 1);
            let density = edge_count as f64 / max_edges as f64;
            println!("  📊 Graph Density: {:.3}", density);
        }
        
        println!();
    }

    fn run_queries(&self) {
        println!("🔍 Running Graph Queries:");
        println!();

        // Query 1: Find all engineers
        self.query_engineers();
        
        // Query 2: Find senior employees
        self.query_senior_employees();
        
        // Query 3: Find mentees of Alice
        self.query_mentees("Alice");
        
        // Query 4: Find all management relationships
        self.query_management_chain();
        
        // Query 5: Department statistics
        self.query_department_stats();
    }

    fn query_engineers(&self) {
        println!("📊 Query 1: Find all engineers");
        
        let engineers = self.storage.find_vertices_by_property(|props| {
            if let Some(dept) = props.get("department") {
                matches!(dept, PropertyValue::String(d) if d == "Engineering")
            } else {
                false
            }
        });

        println!("  🏢 Found {} engineers:", engineers.len());
        for (id, props) in engineers {
            if let (Some(name), Some(level)) = (
                props.get("name").and_then(|p| if let PropertyValue::String(s) = p { Some(s) } else { None }),
                props.get("level").and_then(|p| if let PropertyValue::String(s) = p { Some(s) } else { None })
            ) {
                if let Some(age) = props.get("age").and_then(|p| if let PropertyValue::Int64(a) = p { Some(a) } else { None }) {
                    println!("    👤 {} (age: {}, level: {}) - ID: {}", name, age, level, id);
                }
            }
        }
        println!();
    }

    fn query_senior_employees(&self) {
        println!("📊 Query 2: Find senior employees");
        
        let seniors = self.storage.find_vertices_by_property(|props| {
            if let Some(level) = props.get("level") {
                matches!(level, PropertyValue::String(l) if l == "Senior")
            } else {
                false
            }
        });

        println!("  👔 Found {} senior employees:", seniors.len());
        for (id, props) in seniors {
            if let (Some(name), Some(age)) = (
                props.get("name").and_then(|p| if let PropertyValue::String(s) = p { Some(s) } else { None }),
                props.get("age").and_then(|p| if let PropertyValue::Int64(a) = p { Some(a) } else { None })
            ) {
                println!("    👑 {} (age: {}) - ID: {}", name, age, id);
            }
        }
        println!();
    }

    fn query_mentees(&self, mentor_name: &str) {
        println!("📊 Query 3: Find mentees of {}", mentor_name);
        
        // Find mentor's ID
        let mentor_id = self.storage.find_vertices_by_property(|props| {
            if let Some(name) = props.get("name") {
                matches!(name, PropertyValue::String(n) if n == mentor_name)
            } else {
                false
            }
        }).into_iter().next().map(|(id, _)| id);

        if let Some(mentor_id) = mentor_id {
            let mut mentees = Vec::new();
            
            // Find all outgoing "mentors" edges from mentor
for (edge, _edge_props) in self.storage.list_edges() {
                if edge.label == "mentors" && edge.src == mentor_id {
                if let Some(mentee_props) = self.storage.get_vertex(edge.dst) {
                    println!("    👤 Mentee: {:?}", edge.dst);
                    if let Some(name) = mentee_props.get("name") {
                        if let Some(age) = mentee_props.get("age") {
                            println!("      Name: {}, Age: {}", name, age);
                        }
                    }
                    mentees.push(edge.dst);
                }
                }
            }
            
            if mentees.is_empty() {
                println!("  ❌ No mentees found for {}", mentor_name);
            }
        } else {
            println!("  ❌ Mentor '{}' not found", mentor_name);
        }
        println!();
    }

    fn query_management_chain(&self) {
        println!("📊 Query 4: Management relationships");
        
        let mut management_relationships = Vec::new();
        
        for (edge, edge_props) in self.storage.list_edges() {
            if edge.label == "manages" || edge.label == "reports_to" {
                if let (Some(manager_props), Some(employee_props)) = (
                    self.storage.get_vertex(edge.src),
                    self.storage.get_vertex(edge.dst)
                ) {
                    management_relationships.push((edge, manager_props, employee_props, edge_props));
                }
            }
        }

        println!("  👔 Found {} management relationships:", management_relationships.len());
        for (_edge, manager_props, employee_props, edge_props) in management_relationships {
            let unknown_name = "Unknown".to_string();
            let manager_name = manager_props.get("name").and_then(|p| {
                if let PropertyValue::String(s) = p { Some(s) } else { None }
            }).unwrap_or(&unknown_name);
            let employee_name = employee_props.get("name").and_then(|p| {
                if let PropertyValue::String(s) = p { Some(s) } else { None }
            }).unwrap_or(&unknown_name);
            
            if let Some(since) = edge_props.get("since") {
                println!("    👑 {} manages {} (since: {})", manager_name, employee_name, since);
            }
        }
        println!();
    }

    fn query_department_stats(&self) {
        println!("📊 Query 5: Department statistics");
        
        let all_vertices = self.storage.list_vertices();
        let mut dept_counts: HashMap<String, usize> = HashMap::new();

        for (_, props) in all_vertices {
            if let Some(dept) = props.get("department") {
                if let PropertyValue::String(dept_name) = dept {
                    *dept_counts.entry(dept_name.clone()).or_insert(0) += 1;
                }
            }
        }

        println!("  📊 Department breakdown:");
        let mut total = 0;
        for (dept, count) in &dept_counts {
            println!("    🏢 {}: {} people", dept, count);
            total += count;
        }

        println!("  📈 Total: {} people across {} departments", total, dept_counts.len());
        println!();
    }

    fn run_interactive(&self) {
        println!("🎮 Interactive Mode");
        println!("Commands: 'stats', 'list', 'find <name>', 'help', 'quit'");
        println!();

        loop {
            print!("gdb> ");
            io::stdout().flush().unwrap();

            let mut input = String::new();
            io::stdin().read_line(&mut input).unwrap();
            let input = input.trim();

            match input {
                "quit" | "exit" => {
                    println!("👋 Goodbye!");
                    break;
                }
                "help" => {
                    self.show_help();
                }
                "stats" => {
                    self.show_stats();
                }
                "list" => {
                    self.list_all_data();
                }
                input if input.starts_with("find ") => {
                    let name = &input[5..];
                    self.find_person(name);
                }
                _ => {
                    println!("❌ Unknown command: {}. Type 'help' for assistance.", input);
                }
            }
        }
    }

    fn show_help(&self) {
        println!("📖 Graph Database Commands:");
        println!("  help           - Show this help message");
        println!("  stats          - Show database statistics");
        println!("  list          - List all vertices and edges");
        println!("  find <name>   - Find person by name");
        println!("  quit/exit      - Exit the program");
        println!();
        println!("Example queries:");
        println!("  find Alice    - Find person named Alice");
        println!("  list          - Show all data");
        println!("  stats         - Show statistics");
    }

    fn list_all_data(&self) {
        println!("📋 All Vertices:");
        let vertices = self.storage.list_vertices();
        for (id, props) in vertices {
            if let Some(name) = props.get("name") {
                if let Some(age) = props.get("age") {
                    if let Some(dept) = props.get("department") {
                        println!("  👤 {} (ID: {}, Age: {}, Dept: {})", name, id, age, dept);
                    }
                }
            }
        }

        println!();
        println!("📋 All Edges:");
        let edges = self.storage.list_edges();
        for (edge, edge_props) in edges {
            let src_name = self.get_vertex_name(edge.src);
            let dst_name = self.get_vertex_name(edge.dst);
            
            println!("  {} -[{}]-> {}", src_name, edge.label, dst_name);
            if let Some(since) = edge_props.get("since") {
                println!("      Since: {}", since);
            }
        }
    }

    fn get_vertex_name(&self, id: VertexId) -> String {
        match self.storage.get_vertex(id) {
            Some(props) => {
                props.get("name")
                    .and_then(|p| if let PropertyValue::String(s) = p { Some(s) } else { None })
                    .cloned()
                    .unwrap_or_else(|| "Unknown".to_string())
            }
            None => "Unknown".to_string(),
        }
    }

    fn find_person(&self, name: &str) {
        println!("🔍 Searching for: {}", name);
        
        let matches = self.storage.find_vertices_by_property(|props| {
            if let Some(person_name) = props.get("name") {
                matches!(person_name, PropertyValue::String(n) if n == name)
            } else {
                false
            }
        });

        if matches.is_empty() {
            println!("  ❌ Person '{}' not found", name);
        } else {
            println!("  ✅ Found {} match(es):", matches.len());
            for (id, props) in matches {
                println!("    👤 ID: {}", id);
                for (key, value) in props {
                    println!("      {}: {}", key, value);
                }
            }
        }
    }
}

fn main() {
    println!("🚀 Graph Database Standalone Demo");
    println!("💾 Self-Contained Implementation");
    println!("🔍 Interactive Query System");
    println!("📊 Real-time Analytics");
    println!();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let demo = GraphDemo::new();

    if args.len() > 1 {
        match args[1].as_str() {
            "--demo" => demo.run_demo(),
            "--interactive" => demo.run_interactive(),
            _ => {
                println!("Usage: {} [--demo|--interactive]", args[0]);
                println!("  --demo         Run demonstration with sample data");
                println!("  --interactive  Run interactive query mode");
            }
        }
    } else {
        println!("Usage: {} [--demo|--interactive]", args[0]);
        println!("Running demo by default...");
        demo.run_demo();
    }
}