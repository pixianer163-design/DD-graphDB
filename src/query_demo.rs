//! Query Engine Demo
//!
//! This demo demonstrates the query execution engine with various GQL queries

use std::sync::Arc;

use graph_core::{VertexId, Edge, props};
use graph_storage::{GraphStorage, GraphOperation};
use graph_query::{
    QueryExecutor, Statement,
    NodePattern, EdgePattern, GraphPattern, EdgeDirection,
    Expression, GQLValue, ComparisonOp, LogicalOp, ReturnItem,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Graph Query Engine Demo");
    println!("🔍 Testing GQL Query Execution");
    
    // Create test data
    let storage = create_test_data()?;
    let executor = QueryExecutor::new(storage);
    
    println!("\n✅ Test data created successfully\n");
    
    // Test 1: Property filter query
    test_property_filter(&executor);
    
    // Test 2: Relationship traversal
    test_relationship_traversal(&executor);
    
    // Test 3: Multi-condition query
    test_multi_condition(&executor);
    
    // Test 4: Path query
    test_path_query(&executor)?;
    
    println!("\n🎉 All query tests completed!\n");
    
    Ok(())
}

fn create_test_data() -> Result<Arc<GraphStorage>, Box<dyn std::error::Error>> {
    let temp_dir = std::env::temp_dir().join("query_demo_db");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir)?;
    
    let storage = Arc::new(GraphStorage::new(&temp_dir)?);
    let mut transaction = storage.begin_transaction()?;
    
    // Create Alice (Software Engineer, Senior, age 30)
    transaction.add_operation(GraphOperation::AddVertex {
        id: VertexId::new(1),
        properties: props::map(vec![
            ("name", "Alice"),
            ("type", "Person"),
            ("role", "Software Engineer"),
            ("level", "Senior"),
            ("age", 30i64),
            ("department", "Engineering"),
        ]),
    });
    
    // Create Bob (Software Engineer, Junior, age 25)
    transaction.add_operation(GraphOperation::AddVertex {
        id: VertexId::new(2),
        properties: props::map(vec![
            ("name", "Bob"),
            ("type", "Person"),
            ("role", "Software Engineer"),
            ("level", "Junior"),
            ("age", 25i64),
            ("department", "Engineering"),
        ]),
    });
    
    // Create Charlie (Product Manager, age 35)
    transaction.add_operation(GraphOperation::AddVertex {
        id: VertexId::new(3),
        properties: props::map(vec![
            ("name", "Charlie"),
            ("type", "Person"),
            ("role", "Product Manager"),
            ("age", 35i64),
            ("department", "Product"),
        ]),
    });
    
    // Create David (Designer, age 28)
    transaction.add_operation(GraphOperation::AddVertex {
        id: VertexId::new(4),
        properties: props::map(vec![
            ("name", "David"),
            ("type", "Person"),
            ("role", "Designer"),
            ("age", 28i64),
            ("department", "Design"),
        ]),
    });
    
    // Create Eve (CTO, age 40)
    transaction.add_operation(GraphOperation::AddVertex {
        id: VertexId::new(5),
        properties: props::map(vec![
            ("name", "Eve"),
            ("type", "Person"),
            ("role", "CTO"),
            ("age", 40i64),
            ("department", "Engineering"),
        ]),
    });
    
    // Create relationships
    // Alice manages Bob
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(1), VertexId::new(2), "manages"),
        properties: props::map(vec![("since", 2022i64)]),
    });
    
    // Bob works with Charlie
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(2), VertexId::new(3), "collaborates"),
        properties: props::map(vec![("project", "ProductA")]),
    });
    
    // Charlie works with David
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(3), VertexId::new(4), "collaborates"),
        properties: props::map(vec![("project", "ProductA")]),
    });
    
    // Eve manages Alice
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(5), VertexId::new(1), "manages"),
        properties: props::map(vec![("since", 2020i64)]),
    });
    
    // Alice friends with Charlie
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(1), VertexId::new(3), "friend"),
        properties: props::map(vec![("since", 2021i64)]),
    });
    
    storage.commit_transaction(transaction)?;
    
    println!("📊 Created 5 vertices and 5 edges");
    
    Ok(storage)
}

fn test_property_filter(executor: &QueryExecutor) {
    println!("📍 Test 1: Property Filter Query");
    println!("   Query: MATCH (v:Person) WHERE v.age > 25 RETURN v.name, v.age");
    
    // Build query programmatically
    let pattern = GraphPattern {
        nodes: vec![NodePattern {
            variable: Some("v".to_string()),
            label: Some("Person".to_string()),
            properties: std::collections::HashMap::new(),
        }],
        edges: vec![],
    };
    
    let where_clause = Some(Expression::Comparison {
        left: Box::new(Expression::PropertyAccess("v".to_string(), "age".to_string())),
        operator: ComparisonOp::Greater,
        right: Box::new(Expression::Literal(GQLValue::Number(25.0))),
    });
    
    let return_items = vec![
        ReturnItem::Property("v".to_string(), "name".to_string()),
        ReturnItem::Property("v".to_string(), "age".to_string()),
    ];
    
    let statement = Statement::Match {
        pattern,
        where_clause,
        return_items,
    };
    
    match executor.execute(statement) {
        Ok(result) => {
            println!("   ✅ Result: {}", result.format());
            println!("   📊 Found {} matching vertices\n", result.count());
        }
        Err(e) => {
            println!("   ❌ Error: {}\n", e);
        }
    }
}

fn test_relationship_traversal(executor: &QueryExecutor) {
    println!("📍 Test 2: Relationship Traversal Query");
    println!("   Query: MATCH (a)-[e:manages]->(b) WHERE a.name = 'Alice' RETURN b.name");
    
    let pattern = GraphPattern {
        nodes: vec![
            NodePattern {
                variable: Some("a".to_string()),
                label: None,
                properties: [("name".to_string(), GQLValue::String("Alice".to_string()))]
                    .into_iter()
                    .collect(),
            },
            NodePattern {
                variable: Some("b".to_string()),
                label: None,
                properties: std::collections::HashMap::new(),
            },
        ],
        edges: vec![EdgePattern {
            variable: Some("e".to_string()),
            label: Some("manages".to_string()),
            properties: std::collections::HashMap::new(),
            direction: EdgeDirection::Outgoing,
        }],
    };
    
    let return_items = vec![ReturnItem::Property("b".to_string(), "name".to_string())];
    
    let statement = Statement::Match {
        pattern,
        where_clause: None,
        return_items,
    };
    
    match executor.execute(statement) {
        Ok(result) => {
            println!("   ✅ Result: {}", result.format());
            println!("   📊 Found {} managed by Alice\n", result.count());
        }
        Err(e) => {
            println!("   ❌ Error: {}\n", e);
        }
    }
}

fn test_multi_condition(executor: &QueryExecutor) {
    println!("📍 Test 3: Multi-Condition Query");
    println!("   Query: MATCH (v:Person) WHERE v.age > 25 AND v.department = 'Engineering' RETURN v.name");
    
    let pattern = GraphPattern {
        nodes: vec![NodePattern {
            variable: Some("v".to_string()),
            label: Some("Person".to_string()),
            properties: std::collections::HashMap::new(),
        }],
        edges: vec![],
    };
    
    let where_clause = Some(Expression::Logical {
        left: Box::new(Expression::Comparison {
            left: Box::new(Expression::PropertyAccess("v".to_string(), "age".to_string())),
            operator: ComparisonOp::Greater,
            right: Box::new(Expression::Literal(GQLValue::Number(25.0))),
        }),
        operator: LogicalOp::And,
        right: Box::new(Expression::Comparison {
            left: Box::new(Expression::PropertyAccess("v".to_string(), "department".to_string())),
            operator: ComparisonOp::Equal,
            right: Box::new(Expression::Literal(GQLValue::String("Engineering".to_string()))),
        }),
    });
    
    let return_items = vec![ReturnItem::Property("v".to_string(), "name".to_string())];
    
    let statement = Statement::Match {
        pattern,
        where_clause,
        return_items,
    };
    
    match executor.execute(statement) {
        Ok(result) => {
            println!("   ✅ Result: {}", result.format());
            println!("   📊 Found {} people >25 in Engineering\n", result.count());
        }
        Err(e) => {
            println!("   ❌ Error: {}\n", e);
        }
    }
}

fn test_path_query(executor: &QueryExecutor) -> Result<(), Box<dyn std::error::Error>> {
    println!("📍 Test 4: Path Query (Using Storage API)");
    println!("   Query: Find shortest path from Alice to David");
    
    // Use storage API directly for path finding
    let storage = Arc::new(GraphStorage::new(std::env::temp_dir().join("query_demo_db"))?);
    
    let start = VertexId::new(1); // Alice
    let end = VertexId::new(4);   // David
    
    match storage.shortest_path(start, end, 5) {
        Ok(Some(path)) => {
            let path_names: Vec<String> = path
                .iter()
                .filter_map(|id| storage.get_vertex(*id).ok().flatten())
                .filter_map(|props| props.get("name").and_then(|p| p.as_string()).map(|s| s.to_string()))
                .collect();
            
            println!("   ✅ Path found: {:?}", path_names);
            println!("   📊 Path length: {} hops\n", path.len() - 1);
        }
        Ok(None) => {
            println!("   ℹ️  No path found between Alice and David\n");
        }
        Err(e) => {
            println!("   ❌ Error: {}\n", e);
        }
    }
    
    Ok(())
}
