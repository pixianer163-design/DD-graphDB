//! Simple Query Engine Test
//!
//! This test verifies the query executor without requiring pest parser

use std::sync::Arc;
use std::collections::HashMap;

use graph_core::{VertexId, Edge, props};
use graph_storage::{GraphStorage, GraphOperation};
use graph_query::{
    QueryExecutor, Statement, QueryResult,
    NodePattern, EdgePattern, GraphPattern, EdgeDirection,
    Expression, GQLValue, ComparisonOp, LogicalOp, ReturnItem,
};

fn main() {
    println!("🚀 Query Engine Core Test");
    println!("============================\n");
    
    // Create test storage
    let storage = match create_test_storage() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("❌ Failed to create test storage: {}", e);
            return;
        }
    };
    
    let executor = QueryExecutor::new(storage);
    
    // Test 1: Simple property filter
    println!("📍 Test 1: Property Filter (age > 25)");
    test_property_filter(&executor);
    
    // Test 2: Relationship traversal
    println!("📍 Test 2: Relationship Traversal (manages)");
    test_relationship_traversal(&executor);
    
    // Test 3: Multi-condition query
    println!("📍 Test 3: Multi-Condition (age > 25 AND department = Engineering)");
    test_multi_condition(&executor);
    
    println!("\n✅ All core tests completed!");
}

fn create_test_storage() -> Result<Arc<GraphStorage>, Box<dyn std::error::Error>> {
    let temp_dir = std::env::temp_dir().join("query_test_simple");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir)?;
    
    let storage = Arc::new(GraphStorage::new(&temp_dir)?);
    let mut transaction = storage.begin_transaction()?;
    
    // Alice (30, Engineering)
    transaction.add_operation(GraphOperation::AddVertex {
        id: VertexId::new(1),
        properties: props::map(vec![
            ("name", "Alice"),
            ("type", "Person"),
            ("age", 30 as i64),
            ("department", "Engineering"),
        ]),
    });
    
    // Bob (25, Engineering)
    transaction.add_operation(GraphOperation::AddVertex {
        id: VertexId::new(2),
        properties: props::map(vec![
            ("name", "Bob"),
            ("type", "Person"),
            ("age", 25 as i64),
            ("department", "Engineering"),
        ]),
    });
    
    // Charlie (35, Product)
    transaction.add_operation(GraphOperation::AddVertex {
        id: VertexId::new(3),
        properties: props::map(vec![
            ("name", "Charlie"),
            ("type", "Person"),
            ("age", 35 as i64),
            ("department", "Product"),
        ]),
    });
    
    // David (28, Design)
    transaction.add_operation(GraphOperation::AddVertex {
        id: VertexId::new(4),
        properties: props::map(vec![
            ("name", "David"),
            ("type", "Person"),
            ("age", 28 as i64),
            ("department", "Design"),
        ]),
    });
    
    // Eve (40, Engineering)
    transaction.add_operation(GraphOperation::AddVertex {
        id: VertexId::new(5),
        properties: props::map(vec![
            ("name", "Eve"),
            ("type", "Person"),
            ("age", 40 as i64),
            ("department", "Engineering"),
        ]),
    });
    
    // Alice manages Bob
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(1), VertexId::new(2), "manages"),
        properties: props::map(vec![]),
    });
    
    // Eve manages Alice
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(5), VertexId::new(1), "manages"),
        properties: props::map(vec![]),
    });
    
    // Bob collaborates with Charlie
    transaction.add_operation(GraphOperation::AddEdge {
        edge: Edge::new(VertexId::new(2), VertexId::new(3), "collaborates"),
        properties: props::map(vec![]),
    });
    
    storage.commit_transaction(transaction)?;
    
    Ok(storage)
}

fn test_property_filter(executor: &QueryExecutor) {
    // Build query: MATCH (v:Person) WHERE v.age > 25 RETURN v.name
    let pattern = GraphPattern {
        nodes: vec![NodePattern {
            variable: Some("v".to_string()),
            label: Some("Person".to_string()),
            properties: HashMap::new(),
        }],
        edges: vec![],
    };
    
    let where_clause = Some(Expression::Comparison {
        left: Box::new(Expression::PropertyAccess("v".to_string(), "age".to_string())),
        operator: ComparisonOp::Greater,
        right: Box::new(Expression::Literal(GQLValue::Number(25.0))),
    });
    
    let return_items = vec![ReturnItem::Property("v".to_string(), "name".to_string())];
    
    let statement = Statement::Match {
        pattern,
        where_clause,
        return_items,
    };
    
    match executor.execute(statement) {
        Ok(result) => {
            println!("   ✅ Found {} people with age > 25", result.count());
            println!("   📊 Result: {}", result.format());
        }
        Err(e) => println!("   ❌ Error: {}", e),
    }
    println!();
}

fn test_relationship_traversal(executor: &QueryExecutor) {
    // Build query: MATCH (a)-[e:manages]->(b) WHERE a.name = 'Alice' RETURN b.name
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), GQLValue::String("Alice".to_string()));
    
    let pattern = GraphPattern {
        nodes: vec![
            NodePattern {
                variable: Some("a".to_string()),
                label: None,
                properties: alice_props,
            },
            NodePattern {
                variable: Some("b".to_string()),
                label: None,
                properties: HashMap::new(),
            },
        ],
        edges: vec![EdgePattern {
            variable: Some("e".to_string()),
            label: Some("manages".to_string()),
            properties: HashMap::new(),
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
            println!("   ✅ Found {} people managed by Alice", result.count());
            println!("   📊 Result: {}", result.format());
        }
        Err(e) => println!("   ❌ Error: {}", e),
    }
    println!();
}

fn test_multi_condition(executor: &QueryExecutor) {
    // Build query: MATCH (v:Person) WHERE v.age > 25 AND v.department = 'Engineering' RETURN v.name
    let pattern = GraphPattern {
        nodes: vec![NodePattern {
            variable: Some("v".to_string()),
            label: Some("Person".to_string()),
            properties: HashMap::new(),
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
            println!("   ✅ Found {} people >25 in Engineering", result.count());
            println!("   📊 Result: {}", result.format());
        }
        Err(e) => println!("   ❌ Error: {}", e),
    }
    println!();
}
