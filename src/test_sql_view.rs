//! Test SQL view creation and storage

use std::sync::Arc;
use tempfile::TempDir;
use chrono::Utc;
use graph_views::{
    ViewRegistry, ViewStore, ViewDefinition, ViewStorageInfo, StorageFormat,
    MaterializedView, ViewType, RefreshPolicy, ViewSizeMetrics
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing SQL view creation and storage...");
    
    // Create temporary directory for view storage
    let temp_dir = TempDir::new()?;
    let views_dir = temp_dir.path().join("views");
    let data_dir = temp_dir.path().join("data");
    
    // Create view registry
    let registry = ViewRegistry::new(views_dir.to_str().unwrap())?;
    
    // Create a SQL view definition
    let sql_query = "SELECT * FROM vertices WHERE type = 'user'".to_string();
    
    // Create materialized view with SQL type
    let view_type = ViewType::sql_query(sql_query.clone())?;
    let view = MaterializedView::new(
        "user_vertices".to_string(),
        view_type.clone(),
        RefreshPolicy::OnDemand { ttl_ms: 300000 },
        ViewSizeMetrics::default(),
    );
    
    // Create view definition
    let view_def = ViewDefinition {
        name: "user_vertices".to_string(),
        view_type,
        refresh_policy: RefreshPolicy::OnDemand { ttl_ms: 300000 },
        size_metrics: ViewSizeMetrics::default(),
        dsl_definition: Some(sql_query.clone()),
        created_at: Utc::now(),
        last_refreshed: Utc::now(),
        dependencies: vec![],
        storage_info: ViewStorageInfo::Disk { 
            path: data_dir.clone(), 
            format: StorageFormat::Bincode 
        },
    };
    
    // Save view definition
    registry.save_view(&view_def).await?;
    println!("View definition saved.");
    
    // Load view definition
    let loaded_def = registry.load_view("user_vertices").await?;
    println!("View definition loaded: {}", loaded_def.name);
    
    // Create view store for materialized data
    let store = ViewStore::new(data_dir.to_str().unwrap())?;
    
    // Simulate some view data
    use graph_views::ViewCacheData;
    let dummy_data = ViewCacheData::VertexLookup {
        vertices: std::collections::HashMap::new(),
    };
    
    // Save view data
    store.save_view_data("user_vertices", &dummy_data).await?;
    println!("View data saved.");
    
    // Load view data
    let loaded_data = store.load_view_data("user_vertices").await?;
    println!("View data loaded: {:?}", loaded_data.is_some());
    
    // Test SQL parsing
    use graph_views::sql_parser::SqlParser;
    let parser = SqlParser::new();
    let stmt = parser.parse(&sql_query)?;
    println!("SQL parsed successfully: {}", parser.explain(&stmt));
    
    println!("All tests passed!");
    Ok(())
}