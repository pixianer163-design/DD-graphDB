//! Incremental Computation Quick Demo
//!
//! This program demonstrates the incremental computation capabilities
//! of the graph database with materialized views.

use std::sync::Arc;
use graph_views::{
    IncrementalDemo, MultiLevelCacheConfig
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Graph Database - Incremental Computation Demo");
    println!("📊 Demonstrating real-time updates with differential dataflow");
    println!();

    // Create incremental demonstration
    let mut demo = IncrementalDemo::new();

    // Run quick demo
    demo.run_quick_demo().await?;

    println!();
    println!("✅ Incremental Computation Demo Completed Successfully!");
    println!("🎯 Key Features Demonstrated:");
    println!("   🔄 Real-time change processing");
    println!("   📈 Incremental view updates");
    println!("   🧮 Minimal recomputation overhead");
    println!("   🗄️  Multi-level cache integration");
    println!();
    println!("💡 To run the full comprehensive demo, use:");
    println!("   cargo run --features views --bin materialized_views_demo");

    Ok(())
}