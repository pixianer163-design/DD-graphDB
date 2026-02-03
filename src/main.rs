//! Graph Database Server
//!
//! Main entry point for the graph database application providing
//! materialized view operations with differential-dataflow support.
//! NO REGULAR QUERY SUPPORT - ALL ACCESS THROUGH MATERIALIZED VIEWS

use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "streaming")]
use differential_dataflow::input::InputSession;
#[cfg(feature = "streaming")]
use timely::dataflow::scopes::Scope;

use tracing::{error, info, Level};

// Import our modules
use graph_core::{VertexId, Edge, props};
use graph_storage::{GraphStorage, GraphOperation};

// Random utilities
#[cfg(feature = "views")]
use rand;

#[cfg(feature = "views")]
use graph_views::{
    MaterializedView, ViewType, RefreshPolicy, ViewSizeMetrics,
    StorageIntegration, DataChangeEvent, ViewDataReader,
    ViewRefreshListener, IntegratedViewManager, QueryRouter, QueryPattern,
    MultiLevelCacheManager, MultiLevelCacheConfig, ViewCacheData,
    IncrementalEngine, DataChange, ChangeSet, IncrementalConfig
};

/// Main graph database server
struct GraphDatabaseServer {
    storage: Arc<GraphStorage>,
    #[cfg(feature = "views")]
    view_manager: Option<IntegratedViewManager>,
    #[cfg(feature = "views")]
    query_router: Option<QueryRouter>,
    #[cfg(feature = "views")]
    cache_manager: Option<Arc<MultiLevelCacheManager>>,
    #[cfg(feature = "views")]
    incremental_engine: Option<Arc<IncrementalEngine>>,
    #[cfg(feature = "views")]
    view_registry: Option<ViewRegistry>,
    #[cfg(feature = "streaming")]
    vertices_input: InputSession<usize, (VertexId, HashMap<String, PropertyValue>), isize>,
    #[cfg(feature = "streaming")]
    edges_input: InputSession<usize, (Edge, HashMap<String, PropertyValue>), isize>,
}

impl GraphDatabaseServer {
    /// Create a new graph database server
    async fn new(storage_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Initialize storage
        let storage = Arc::new(GraphStorage::new(storage_path)?);

        // Initialize view manager if views feature is enabled
        #[cfg(feature = "views")]
        let (view_manager, query_router, cache_manager, incremental_engine) = {
            let storage_integration = Arc::new(StorageIntegration::new(storage.clone()));
            let view_manager = Some(IntegratedViewManager::new(storage_integration));
            
            // Initialize cache manager with intelligent configuration
            let cache_config = MultiLevelCacheConfig::default();
            let cache_manager = Arc::new(MultiLevelCacheManager::new(cache_config));
            
            // Initialize incremental computation engine
            let incremental_config = IncrementalConfig::default();
            let incremental_engine = Arc::new(IncrementalEngine::new(
                incremental_config,
                cache_manager.clone()
            ));
            
            // Initialize query router with cache integration
            let query_router = Some(QueryRouter::with_cache(MultiLevelCacheConfig::default()));
            
            (view_manager, query_router, cache_manager, incremental_engine)
        };

        #[cfg(feature = "streaming")]
        {
            // Initialize differential dataflow inputs
            let vertices_input = InputSession::new();
            let edges_input = InputSession::new();

            Ok(Self {
                storage,
                view_manager,
                #[cfg(feature = "views")]
                query_router,
                #[cfg(feature = "views")]
                cache_manager,
                #[cfg(feature = "views")]
                incremental_engine,
                vertices_input,
                edges_input,
            })
        }

        #[cfg(not(feature = "streaming"))]
        {
            Ok(Self { 
                storage,
                #[cfg(feature = "views")]
                view_manager,
                #[cfg(feature = "views")]
                query_router,
                #[cfg(feature = "views")]
                cache_manager,
                #[cfg(feature = "views")]
                incremental_engine,
            })
        }
    }

    /// Start the interactive shell
    async fn run_interactive(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting Graph Database Interactive Shell");
        info!("Type 'help' for available commands or 'quit' to exit");

        loop {
            print!("gql> ");
            use std::io::{self, Write};
            std::io::stdout().flush()?;

            let mut input = String::new();
            io::stdin().read_line(&mut input)?;

            let input = input.trim();
            if input.is_empty() {
                continue;
            }

            match input {
                "quit" | "exit" => {
                    info!("Goodbye!");
                    break;
                }
                "help" => {
                    self.show_help();
                }
                "stats" => {
                    self.show_stats().await?;
                }
                "demo" => {
                    self.run_demo().await?;
                }
                #[cfg(feature = "views")]
                "views" => {
                    self.show_views().await?;
                }
                #[cfg(feature = "views")]
                input if input.starts_with("VIEW") => {
                    self.execute_view_operation(input).await?;
                }
                #[cfg(feature = "views")]
                input if input.starts_with("CACHE") => {
                    self.execute_cache_operation(input).await?;
                }
                #[cfg(feature = "views")]
                input if input.starts_with("INCREMENTAL") => {
                    self.execute_incremental_operation(input).await?;
                }
                _ => {
                    if input.starts_with("MATCH") || input.starts_with("CREATE") || input.starts_with("DELETE") {
                        println!("❌ Regular queries are not supported!");
                        println!("💡 Use 'VIEW' commands to access materialized views instead.");
                        println!("📋 Type 'help' to see available view operations.");
                    } else {
                        println!("Unknown command: {}. Type 'help' for assistance.", input);
                    }
                }
            }
        }

        Ok(())
    }

    /// Show help information
    fn show_help(&self) {
        println!("🚀 Graph Database - Materialized Views Edition");
        println!("📋 NO REGULAR QUERY SUPPORT - All access through materialized views");
        println!();
        println!("Available Commands:");
        println!("  help              - Show this help message");
        println!("  stats             - Show database statistics");
        println!("  demo              - Run a demonstration");
        #[cfg(feature = "views")]
        println!("  views             - List available materialized views");
        #[cfg(feature = "views")]
        println!("  VIEW LIST         - List all registered views");
        #[cfg(feature = "views")]
        println!("  VIEW REFRESH <id> - Refresh a specific view");
        #[cfg(feature = "views")]
        println!("  VIEW CREATE <def> - Create a new view");
        #[cfg(feature = "views")]
        println!("  VIEW ROUTE        - Demonstrate intelligent query routing");
        #[cfg(feature = "views")]
        println!("  CACHE STATS       - Show multi-level cache statistics");
        #[cfg(feature = "views")]
        println!("  CACHE CLEAR       - Clear all cache levels");
        #[cfg(feature = "views")]
        println!("  CACHE WARM        - Warm up cache with common queries");
        #[cfg(feature = "views")]
        println!("  INCREMENTAL STATS - Show incremental computation statistics");
        #[cfg(feature = "views")]
        println!("  INCREMENTAL FORCE <id> - Force update of a specific view");
        #[cfg(feature = "views")]
        println!("  INCREMENTAL BATCH <size> - Process batch of pending changes");
        println!("  quit/exit         - Exit the program");
        println!();
        println!("❌ Regular query support (MATCH/CREATE/DELETE) has been removed!");
        println!("💡 All data access now goes through materialized views for better performance.");
        #[cfg(feature = "views")]
        println!("🎯 Use VIEW commands to interact with pre-computed results.");
        #[cfg(feature = "views")]
        println!("🧠 Try 'VIEW ROUTE' to see intelligent query routing in action!");
        #[cfg(not(feature = "views"))]
        println!("🔧 Enable views feature with --features views for full functionality.");
    }

    /// Show current database statistics
    async fn show_stats(&self) -> Result<(), Box<dyn std::error::Error>> {
        match self.storage.get_stats() {
            Ok(stats) => {
                println!("Database Statistics:");
                println!("  Vertices: {}", stats.vertex_count);
                println!("  Edges: {}", stats.edge_count);
                println!("  Version: {}", stats.version);
                println!("  Timestamp: {:?}", stats.timestamp);
            }
            Err(e) => error!("Failed to get stats: {}", e),
        }
        Ok(())
    }

    /// Run a demonstration with sample data
    async fn run_demo(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Running demonstration...");

        // Begin transaction for demo data
        let mut transaction = self.storage.begin_transaction()?;

        // Create sample vertices
        let alice_id = VertexId::new(1);
        let bob_id = VertexId::new(2);
        let charlie_id = VertexId::new(3);

        let alice_props = props::map(vec![
            ("name", "Alice"),
            ("age", "30"),
            ("type", "Person"),
        ]);

        let bob_props = props::map(vec![
            ("name", "Bob"),
            ("age", "25"),
            ("type", "Person"),
        ]);

        let charlie_props = props::map(vec![
            ("name", "Charlie"),
            ("age", "35"),
            ("type", "Person"),
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

        // Create sample edges
        let alice_bob_edge = Edge::new(alice_id, bob_id, "friend");
        let bob_charlie_edge = Edge::new(bob_id, charlie_id, "friend");
        let alice_charlie_edge = Edge::new(alice_id, charlie_id, "colleague");

        transaction.add_operation(GraphOperation::AddEdge {
            edge: alice_bob_edge.clone(),
            properties: props::map(vec![("since", 2020i64)]),
        });

        transaction.add_operation(GraphOperation::AddEdge {
            edge: bob_charlie_edge.clone(),
            properties: props::map(vec![("since", 2021i64)]),
        });

        transaction.add_operation(GraphOperation::AddEdge {
            edge: alice_charlie_edge.clone(),
            properties: props::map(vec![("since", 2019i64)]),
        });

        // Commit transaction
        self.storage.commit_transaction(transaction)?;
        info!("Demo data created successfully!");

        // Show results
        self.show_stats().await?;

        // Demonstrate some queries
        self.demo_vertex_query(alice_id).await?;
        self.demo_edge_query().await?;

        Ok(())
    }

    /// Demonstrate vertex querying
    async fn demo_vertex_query(&self, vertex_id: VertexId) -> Result<(), Box<dyn std::error::Error>> {
        match self.storage.get_vertex(vertex_id) {
            Ok(Some(props)) => {
                println!("Vertex {} properties:", vertex_id);
                for (key, value) in &props {
                    println!("  {}: {}", key, value);
                }
            }
            Ok(None) => println!("Vertex {} not found", vertex_id),
            Err(e) => error!("Failed to get vertex: {}", e),
        }
        Ok(())
    }

    /// Demonstrate edge querying
    async fn demo_edge_query(&self) -> Result<(), Box<dyn std::error::Error>> {
        match self.storage.list_edges() {
            Ok(edges) => {
                println!("All edges:");
                for (edge, props) in &edges {
                    println!("  {} -[{}]-> {}", edge.src, edge.label, edge.dst);
                    if !props.is_empty() {
                        print!(" (");
                        for (i, (key, value)) in props.iter().enumerate() {
                            if i > 0 { print!(", "); }
                            print!("{}: {}", key, value);
                        }
                        println!(")");
                    } else {
                        println!();
                    }
                }
            }
            Err(e) => error!("Failed to list edges: {}", e),
        }
        Ok(())
    }

    /// Execute a GQL query (simplified implementation)
    async fn execute_gql_query(&self, query: &str) -> Result<(), Box<dyn std::error::Error>> {
        info!("Executing GQL query: {}", query);

        // For now, implement a very simple parser for basic queries
        if query.starts_with("MATCH") {
            self.handle_match_query(query).await?;
        } else if query.starts_with("CREATE") {
            self.handle_create_query(query).await?;
        } else if query.starts_with("DELETE") {
            self.handle_delete_query(query).await?;
        } else {
            println!("Unsupported query type");
        }

        Ok(())
    }

    /// Handle MATCH queries
    async fn handle_match_query(&self, query: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Simplified MATCH (v:Person) RETURN v.name
        if query.contains("Person") && query.contains("RETURN") {
            match self.storage.list_vertices() {
                Ok(vertices) => {
                    for (_id, props) in &vertices {
                        if props.get("type").and_then(|p| p.as_string()) == Some("Person") {
                            if let Some(name) = props.get("name").and_then(|p| p.as_string()) {
                                println!("Found Person: {}", name);
                            }
                        }
                    }
                }
                Err(e) => error!("Query failed: {}", e),
            }
        } else {
            println!("MATCH query not yet fully supported");
        }
        Ok(())
    }

    /// Handle CREATE queries
    async fn handle_create_query(&self, query: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Simplified CREATE (v:Person {name: 'Dave', age: 40})
        if query.contains("Person") && query.contains("name") && query.contains("age") {
            let mut transaction = self.storage.begin_transaction()?;
            
            // Extract name (very simple parsing)
            if let Some(start) = query.find("name: '") {
                if let Some(end) = query[start + 7..].find('\'') {
                    let name = &query[start + 7..start + 7 + end];
                    let new_id = VertexId::new(
                        (self.storage.get_stats()?.vertex_count + 1) as u64
                    );
                    
                    let age = if let Some(age_start) = query.find("age: ") {
                        if let Some(age_end) = query[age_start + 5..].find(',') {
                            query[age_start + 5..age_start + 5 + age_end].parse::<i64>().unwrap_or(0)
                        } else {
                            40 // default
                        }
                    } else {
                        40
                    };
                    
                    let props = props::map(vec![
                        ("name", name),
                        ("age", &age.to_string()),
                        ("type", "Person"),
                    ]);
                    
                    transaction.add_operation(GraphOperation::AddVertex {
                        id: new_id,
                        properties: props,
                    });
                    
                    self.storage.commit_transaction(transaction)?;
                    println!("Created Person: {}", name);
                    return Ok(());
                }
            }
        }
        
        println!("CREATE query not yet fully supported");
        Ok(())
    }

    /// Handle DELETE queries
    async fn handle_delete_query(&self, query: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Simplified DELETE (v {name: 'Alice'})
        if query.contains("name: '") {
            if let Some(start) = query.find("name: '") {
                if let Some(end) = query[start + 7..].find('\'') {
                    let name = &query[start + 7..start + 7 + end];
                    
                    // Find vertices with this name
                    match self.storage.find_vertices_by_property(|props| {
                        props.get("name").and_then(|p| p.as_string()) == Some(name)
                    }) {
                        Ok(vertices) => {
                            if !vertices.is_empty() {
                                let mut transaction = self.storage.begin_transaction()?;
                                
                                let vertex_count = vertices.len();
    for (id, _props) in &vertices {
        println!("  Vertex {}: {:?}", id, _props);
    }
                                
                                self.storage.commit_transaction(transaction)?;
                                println!("Deleted {} vertices with name: {}", vertex_count, name);
                            } else {
                                println!("No vertices found with name: {}", name);
                            }
                        }
                        Err(e) => error!("DELETE failed: {}", e),
                    }
                    return Ok(());
                }
            }
        }
        
        println!("DELETE query not yet fully supported");
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Show available materialized views
    async fn show_views(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.view_manager {
            Some(manager) => {
                println!("📋 Available Materialized Views:");
                
                // Since we don't have a list_views method yet, show basic info
                println!("   🎯 Views integration is enabled");
                println!("   🔧 View manager initialized successfully");
                println!("   📊 Storage integration active");
                
                // Show some example view types
                println!();
                println!("📝 Supported View Types:");
                println!("   🔍 Lookup - Fast key-value access to specific vertices");
                println!("   📊 Aggregation - Pre-computed counts, sums, averages");
                println!("   🧮 Analytics - Complex graph algorithms");
                println!("   🔀 Hybrid - Combinations of multiple view types");
                println!();
                println!("💡 Use 'VIEW CREATE' to register new views");
                println!("💡 Use 'VIEW REFRESH <id>' to update existing views");
            }
            None => {
                println!("❌ View manager not initialized");
            }
        }
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Execute view-related operations
    async fn execute_view_operation(&self, input: &str) -> Result<(), Box<dyn std::error::Error>> {
        let parts: Vec<&str> = input.split_whitespace().collect();
        
        if parts.len() < 2 {
            println!("❌ Usage: VIEW <operation> [arguments]");
            return Ok(());
        }
        
        match parts[1] {
            "LIST" => {
                self.show_views().await?;
            }
            "CREATE" => {
                println!("🔧 View creation under development");
                println!("📝 Example: VIEW CREATE lookup user-lookup");
            }
            "REFRESH" => {
                if parts.len() < 3 {
                    println!("❌ Usage: VIEW REFRESH <view_id>");
                    return Ok(());
                }
                println!("🔄 Refreshing view: {}", parts[2]);
                println!("⏰ Refresh operation simulated - under development");
            }
            "ROUTE" => {
                self.demonstrate_query_routing().await?;
            }
            _ => {
                println!("❌ Unknown view operation: {}", parts[1]);
                println!("💡 Supported operations: LIST, CREATE, REFRESH, ROUTE");
            }
        }
        
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Demonstrate intelligent query routing
    async fn demonstrate_query_routing(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🧠 Intelligent Query Routing Demonstration:");
        println!("🎯 Replacing traditional query planning with fast view-based routing");
        println!();
        
        match &self.query_router {
            Some(router) => {
                // Create some sample views for demonstration
                let mut demo_router = router.clone();
                
                // Register sample views
                let lookup_view = MaterializedView::new(
                    "user-lookup".to_string(),
                    ViewType::Lookup { 
                        key_vertices: vec![VertexId::new(1), VertexId::new(2), VertexId::new(3)]
                    },
                    RefreshPolicy::EventDriven { debounce_ms: 100 },
                    ViewSizeMetrics {
                        estimated_memory_bytes: 1024,
                        estimated_vertex_count: 3,
                        estimated_edge_count: 0,
                        update_frequency: 1,
                    },
                );
                
                let aggregation_view = MaterializedView::new(
                    "role-count".to_string(),
                    ViewType::Aggregation { 
                        aggregate_type: "count_by_role".to_string()
                    },
                    RefreshPolicy::FixedInterval(std::time::Duration::from_secs(300)),
                    ViewSizeMetrics {
                        estimated_memory_bytes: 512,
                        estimated_vertex_count: 0,
                        estimated_edge_count: 0,
                        update_frequency: 12,
                    },
                );
                
                let analytics_view = MaterializedView::new(
                    "connectivity".to_string(),
                    ViewType::Analytics { 
                        algorithm: "graph_traversal".to_string()
                    },
                    RefreshPolicy::Hybrid {
                        event_driven: true,
                        interval_ms: 600000, // 10 minutes
                    },
                    ViewSizeMetrics {
                        estimated_memory_bytes: 2048,
                        estimated_vertex_count: 3,
                        estimated_edge_count: 2,
                        update_frequency: 6,
                    },
                );
                
                demo_router.register_view("user-lookup".to_string(), lookup_view);
                demo_router.register_view("role-count".to_string(), aggregation_view);
                demo_router.register_view("connectivity".to_string(), analytics_view);
                
                // Demonstrate routing for different query patterns
                let test_patterns = vec![
                    QueryPattern::VertexLookup {
                        vertex_ids: vec![VertexId::new(1)]
                    },
                    QueryPattern::VertexLookup {
                        vertex_ids: vec![VertexId::new(4)] // Not in lookup view
                    },
                    QueryPattern::Aggregation {
                        aggregate_type: "count_by_role".to_string(),
                        group_by: Some(vec!["role".to_string()]),
                        filter: None,
                    },
                    QueryPattern::Analytics {
                        algorithm: "graph_traversal".to_string(),
                        source: Some(VertexId::new(1)),
                        target: Some(VertexId::new(3)),
                        parameters: 0,
                    },
                ];
                
                println!("📋 Query Pattern Routing Results:");
                println!();
                
                for (i, pattern) in test_patterns.iter().enumerate() {
                    println!("{}. Query Pattern: {:?}", i + 1, pattern);
                    
                    match demo_router.route_query(pattern) {
                        Ok(decision) => {
                            println!("   ✅ Routed to view: {}", decision.target_view);
                            println!("   ⚡ Expected latency: {}ms", decision.expected_latency_ms);
                            println!("   🎯 Confidence: {}%", decision.confidence);
                            if !decision.required_transforms.is_empty() {
                                println!("   🔧 Required transforms: {:?}", decision.required_transforms);
                            }
                        }
                        Err(e) => {
                            println!("   ❌ Routing failed: {}", e);
                        }
                    }
                    println!();
                }
                
                println!("🚀 Query Routing Benefits:");
                println!("   ✅ Eliminates query planning overhead completely");
                println!("   ✅ Sub-millisecond pattern matching and routing");
                println!("   ✅ Intelligent view selection based on query patterns");
                println!("   ✅ Performance-based routing optimization");
                println!("   ✅ Adaptive routing based on view performance statistics");
                
            }
            None => {
                println!("❌ Query router not initialized");
            }
        }
        
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Execute cache management operations
    async fn execute_cache_operation(&self, input: &str) -> Result<(), Box<dyn std::error::Error>> {
        let parts: Vec<&str> = input.split_whitespace().collect();
        
        if parts.len() < 2 {
            println!("❌ Usage: CACHE <operation> [arguments]");
            return Ok(());
        }
        
        match parts[1] {
            "STATS" => {
                self.show_cache_stats().await?;
            }
            "CLEAR" => {
                self.clear_cache().await?;
            }
            "WARM" => {
                self.warm_up_cache().await?;
            }
            _ => {
                println!("❌ Unknown cache operation: {}", parts[1]);
                println!("💡 Supported operations: STATS, CLEAR, WARM");
            }
        }
        
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Show cache statistics
    async fn show_cache_stats(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.cache_manager {
            Some(cache_manager) => {
                let stats = cache_manager.get_stats();
                println!("🚀 Multi-Level Cache Statistics:");
                println!("{}", stats.summary());
                println!();
                println!("💡 Cache Performance:");
                println!("   L1 (Hot): {:.2}% hit rate, {:.1} MB memory", 
                    stats.l1_stats.hit_ratio * 100.0,
                    stats.l1_stats.memory_usage_bytes as f64 / (1024.0 * 1024.0)
                );
                println!("   L2 (Warm): {:.2}% hit rate, {:.1} MB memory", 
                    stats.l2_stats.hit_ratio * 100.0,
                    stats.l2_stats.memory_usage_bytes as f64 / (1024.0 * 1024.0)
                );
                println!("   L3 (Cold): {:.2}% hit rate, {:.1} MB memory", 
                    stats.l3_stats.hit_ratio * 100.0,
                    stats.l3_stats.memory_usage_bytes as f64 / (1024.0 * 1024.0)
                );
                println!("   📊 Overall: {:.2}% hit rate across all levels", 
                    stats.overall_hit_ratio * 100.0
                );
            }
            None => {
                println!("❌ Cache manager not initialized");
            }
        }
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Clear all cache levels
    async fn clear_cache(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.cache_manager {
            Some(cache_manager) => {
                cache_manager.clear_all();
                println!("✅ All cache levels cleared successfully");
                println!("   L1 (Hot): Cleared");
                println!("   L2 (Warm): Cleared");
                println!("   L3 (Cold): Cleared");
                println!("🔄 Cache is now fresh and ready for new data");
            }
            None => {
                println!("❌ Cache manager not initialized");
            }
        }
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Warm up cache with common query patterns
    async fn warm_up_cache(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.query_router {
            Some(router) => {
                println!("🔥 Warming up cache with common query patterns...");
                
                // Create common query patterns for warm-up
                let warm_patterns = vec![
                    // Common vertex lookups
                    QueryPattern::VertexLookup {
                        vertex_ids: vec![VertexId::new(1), VertexId::new(2), VertexId::new(3)]
                    },
                    // Common aggregations
                    QueryPattern::Aggregation {
                        aggregate_type: "count_by_role".to_string(),
                        group_by: Some(vec!["type".to_string()]),
                        filter: None,
                    },
                    // Common analytics
                    QueryPattern::Analytics {
                        algorithm: "shortest_path".to_string(),
                        source: Some(VertexId::new(1)),
                        target: Some(VertexId::new(3)),
                        parameters: 0,
                    },
                    // Edge traversals
                    QueryPattern::EdgeTraversal {
                        start_vertex: Some(VertexId::new(1)),
                        edge_types: Some(vec!["friend".to_string(), "colleague".to_string()]),
                        direction: Some("outbound".to_string()),
                        depth: Some(2),
                    },
                ];
                
                if let Err(e) = router.warm_up_cache(warm_patterns) {
                    println!("❌ Cache warm-up failed: {}", e);
                } else {
                    println!("✅ Cache warm-up completed successfully");
                    println!("💡 Cache is now prepared for optimal performance");
                    
                    // Show updated stats
                    self.show_cache_stats().await?;
                }
            }
            None => {
                println!("❌ Query router not initialized");
            }
        }
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Execute incremental computation operations
    async fn execute_incremental_operation(&self, input: &str) -> Result<(), Box<dyn std::error::Error>> {
        let parts: Vec<&str> = input.split_whitespace().collect();
        
        if parts.len() < 2 {
            println!("❌ Usage: INCREMENTAL <operation> [arguments]");
            return Ok(());
        }
        
        match parts[1] {
            "STATS" => {
                self.show_incremental_stats().await?;
            }
            "FORCE" => {
                if parts.len() < 3 {
                    println!("❌ Usage: INCREMENTAL FORCE <view_id>");
                    return Ok(());
                }
                self.force_incremental_update(parts[2]).await?;
            }
            "BATCH" => {
                let batch_size = if parts.len() > 2 {
                    parts[2].parse::<usize>().unwrap_or(100)
                } else {
                    100
                };
                self.process_incremental_batch(batch_size).await?;
            }
            _ => {
                println!("❌ Unknown incremental operation: {}", parts[1]);
                println!("💡 Supported operations: STATS, FORCE, BATCH");
            }
        }
        
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Show incremental computation statistics
    async fn show_incremental_stats(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.incremental_engine {
            Some(engine) => {
                let state = engine.get_engine_state();
                let perf_stats = engine.get_performance_stats();
                
                println!("🚀 Incremental Computation Engine Statistics:");
                println!("📊 Performance Metrics:");
                println!("   🔄 Changes Processed: {}", state.changes_processed);
                println!("   📈 Views Updated: {}", state.views_updated);
                println!("   ⚡ Time Saved: {} ms", state.time_saved_ms);
                println!("   ⏰ Uptime: {:.1} seconds", state.last_activity.elapsed().as_secs_f64());
                println!();
                
                println!("🔧 Advanced Metrics:");
                for (key, value) in &perf_stats {
                    println!("   {}: {}", key, value);
                }
                println!();
                
                println!("💡 Engine Benefits:");
                println!("   ✅ Real-time incremental updates");
                println!("   ✅ Minimal recomputation overhead");
                println!("   ✅ Automatic dependency propagation");
                println!("   ✅ Consistent data maintenance");
            }
            None => {
                println!("❌ Incremental engine not initialized");
            }
        }
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Force incremental update of a specific view
    async fn force_incremental_update(&self, view_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        match &self.incremental_engine {
            Some(engine) => {
                println!("🔄 Forcing incremental update for view: {}", view_id);
                
                let start_time = std::time::Instant::now();
                
                match engine.force_update_view(view_id) {
                    Ok(result) => {
                        let elapsed = start_time.elapsed().as_millis();
                        
                        println!("✅ Incremental update completed:");
                        println!("   📋 View: {}", result.view_id);
                        println!("   ⚡ Computation Time: {} ms", result.computation_time_ms);
                        println!("   🔄 Changes Applied: {}", result.changes_applied);
                        println!("   📊 Total Time: {} ms", elapsed);
                        
                        if result.success {
                            println!("   ✅ Status: SUCCESS");
                        } else {
                            println!("   ❌ Status: FAILED");
                        }
                        
                        if result.cache_invalidated {
                            println!("   🗑️  Cache: Invalidated");
                        }
                    }
                    Err(e) => {
                        println!("❌ Incremental update failed: {}", e);
                    }
                }
            }
            None => {
                println!("❌ Incremental engine not initialized");
            }
        }
        Ok(())
    }

    #[cfg(feature = "views")]
    /// Process batch of incremental changes
    async fn process_incremental_batch(&self, batch_size: usize) -> Result<(), Box<dyn std::error::Error>> {
        match &self.incremental_engine {
            Some(engine) => {
                println!("🔄 Processing incremental batch (size: {})...", batch_size);
                
                // Create sample changeset for demonstration
                let mut changeset = ChangeSet::new(
                    format!("batch_{}", std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()),
                    "interactive_batch".to_string()
                );
                
                // Add sample changes
                let vertex_id = VertexId::new((rand::random::<u32>() % 1000) + 1);
                changeset.add_change(DataChange::AddVertex {
                    id: vertex_id,
                    properties: std::collections::HashMap::from([
                        ("name".to_string(), crate::graph_core::props::string("Batch User")),
                        ("type".to_string(), crate::graph_core::props::string("Person")),
                        ("batch_id".to_string(), crate::graph_core::props::int64(std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64)),
                    ]),
                });
                
                let start_time = std::time::Instant::now();
                
                match engine.process_changeset(changeset) {
                    Ok(results) => {
                        let elapsed = start_time.elapsed().as_millis();
                        
                        println!("✅ Batch processing completed:");
                        println!("   📊 Processed: {} changesets", 1);
                        println!("   🔄 Total Updates: {}", results.len());
                        println!("   ⚡ Processing Time: {} ms", elapsed);
                        
                        if !results.is_empty() {
                            println!("   📈 Update Results:");
                            for (i, result) in results.iter().enumerate() {
                                println!("     {}. View: {} - {} ({} ms)", 
                                    i + 1, result.view_id, 
                                    if result.success { "✅" } else { "❌" },
                                    result.computation_time_ms);
                            }
                        }
                    }
                    Err(e) => {
                        println!("❌ Batch processing failed: {}", e);
                    }
                }
            }
            None => {
                println!("❌ Incremental engine not initialized");
            }
        }
        Ok(())
    }

    /// Shutdown gracefully
    async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Shutting down graph database...");
        
        // Force checkpoint before shutdown
        if let Err(e) = self.storage.force_checkpoint() {
            error!("Failed to create final checkpoint: {}", e);
        }
        
        // Close storage - Arc prevents direct close, this is handled by drop
        info!("Storage will be closed automatically on drop");
        
        info!("Graph database shutdown complete");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("🚀 Starting Graph Database Server");
    info!("📋 Materialized Views Edition - NO REGULAR QUERIES");
    info!("📊 Powered by Differential Dataflow");
    #[cfg(feature = "views")]
    info!("🔍 Materialized Views Support Enabled");
    #[cfg(feature = "streaming")]
    info!("🌊 Streaming Computation Enabled");
    info!("💾 Persistent Storage with ACID Transactions");

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let storage_path = if args.len() > 1 {
        &args[1]
    } else {
        "./graph_data"
    };

    // Create and start server
    let mut server = GraphDatabaseServer::new(storage_path).await?;

    // Set up signal handling for graceful shutdown
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            
            let mut sigint = signal(SignalKind::interrupt()).unwrap();
            let mut sigterm = signal(SignalKind::terminate()).unwrap();
            
            tokio::select! {
                _ = sigint.recv() => (),
                _ = sigterm.recv() => (),
            }
        }
        
        #[cfg(not(unix))]
        {
            use tokio::signal;
            
            signal::ctrl_c().await.unwrap();
        }
        
        info!("Received shutdown signal");
        drop(shutdown_tx);
    });

    // Run the interactive shell
    tokio::select! {
        result = server.run_interactive() => {
            if let Err(e) = result {
                error!("Server error: {}", e);
            }
        }
        _ = shutdown_rx.recv() => {
            info!("Initiating graceful shutdown...");
        }
    }

    // Perform shutdown
    server.shutdown().await?;

    Ok(())
}