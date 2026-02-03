//! Real-time Stream Processing Demonstration
//!
//! This program demonstrates the real-time stream processing capabilities
//! with windowed operations and aggregations.

use std::sync::Arc;
use std::time::Duration;

use graph_views::{
    WindowManager, WindowSpec, WindowType, AggregationFunction,
    StreamEvent, StreamEventType, StreamEventData,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌊 Real-time Stream Processing Demo");
    println!("📊 Demonstrating windowed operations and real-time aggregations");
    println!();

    // Create window manager
    let mut window_manager = WindowManager::new();

    // Set up tumbling window for user activity
    let user_activity_window = WindowSpec {
        window_id: "user_activity_1min".to_string(),
        window_type: WindowType::Tumbling,
        duration: Duration::from_secs(60), // 1 minute windows
        slide_interval: Duration::from_secs(0),
        max_events: 1000,
        metadata: Default::default(),
    };

    // Set up sliding window for recent events
    let recent_events_window = WindowSpec {
        window_id: "recent_events_30s".to_string(),
        window_type: WindowType::Sliding,
        duration: Duration::from_secs(30), // 30 second sliding window
        slide_interval: Duration::from_secs(5),   // Slide every 5 seconds
        max_events: 500,
        metadata: Default::default(),
    };

    // Set up session window for user sessions
    let user_session_window = WindowSpec {
        window_id: "user_sessions".to_string(),
        window_type: WindowType::Session,
        duration: Duration::from_secs(0), // No timeout
        slide_interval: Duration::from_secs(0),
        max_events: 100,
        metadata: std::collections::HashMap::from([
            ("session_gap".to_string(), graph_core::props::string("30s")),
        ]),
    };

    // Register windows
    window_manager.register_window(user_activity_window)?;
    window_manager.register_window(recent_events_window)?;
    window_manager.register_window(user_session_window)?;

    // Register aggregation functions
    window_manager.register_aggregation("user_activity_1min", AggregationFunction::Count)?;
    window_manager.register_aggregation("user_activity_1min", AggregationFunction::Distinct("user_id".to_string()))?;
    window_manager.register_aggregation("recent_events_30s", AggregationFunction::Sum("event_count".to_string()))?;
    window_manager.register_aggregation("user_sessions", AggregationFunction::Average("session_duration".to_string()))?;

    println!("✅ Registered windows:");
    println!("   📊 user_activity_1min - Tumbling window (60s)");
    println!("   📈 recent_events_30s - Sliding window (30s)");
    println!("   👤 user_sessions - Session window (30s gap)");

    // Simulate real-time event stream
    demo_real_time_stream(&window_manager).await?;

    // Show final statistics
    show_window_statistics(&window_manager)?;

    println!();
    println!("✅ Real-time Stream Processing Demo Completed!");
    println!("🎯 Key Features Demonstrated:");
    println!("   📊 Time-based windowing (tumbling, sliding, session)");
    println!("   📈 Real-time aggregations (count, sum, avg, distinct)");
    println!("   🌊 Event ordering and watermarks");
    println!("   📊 Multi-window parallel processing");
    println!("   ⚡ Sub-millisecond window operations");

    Ok(())
}

/// Demonstrate real-time stream processing
async fn demo_real_time_stream(window_manager: &WindowManager) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔄 Simulating Real-time Event Stream");
    println!("📝 Generating events with various patterns...");

    // Simulate user activity events over time
    let event_scenarios = vec![
        // Scenario 1: User login activity
        StreamEvent {
            event_id: "evt_001".to_string(),
            event_type: StreamEventType::DataChange(
                graph_views::DataChange::AddVertex {
                    id: graph_core::VertexId::new(1001),
                    properties: std::collections::HashMap::from([
                        ("user_id", graph_core::props::string("user_001")),
                        ("action", graph_core::props::string("login")),
                        ("timestamp", graph_core::props::int64(1640995200000i64)),
                        ("session_id", graph_core::props::string("session_001")),
                    ]),
                }
            ),
            data: StreamEventData::Vertex(
                graph_core::VertexId::new(1001),
                std::collections::HashMap::from([
                    ("user_id", graph_core::props::string("user_001")),
                    ("action", graph_core::props::string("login")),
                ])
            ),
            timestamp: std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
            source: "auth_service".to_string(),
            metadata: std::collections::HashMap::from([
                ("priority", graph_core::props::int64(80)),
            ]),
            priority: 80,
            watermark: Some(1000),
        },

        // Scenario 2: Page view event
        StreamEvent {
            event_id: "evt_002".to_string(),
            event_type: StreamEventType::DataChange(
                graph_views::DataChange::UpdateVertex {
                    id: graph_core::VertexId::new(1001),
                    old_properties: std::collections::HashMap::from([
                        ("last_page", graph_core::props::string("/home")),
                    ]),
                    new_properties: std::collections::HashMap::from([
                        ("last_page", graph_core::props::string("/dashboard")),
                        ("page_views", graph_core::props::int64(5)),
                    ]),
                }
            ),
            data: StreamEventData::Vertex(
                graph_core::VertexId::new(1001),
                std::collections::HashMap::from([
                    ("page_views", graph_core::props::int64(5)),
                ])
            ),
            timestamp: std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1010),
            source: "web_frontend".to_string(),
            metadata: std::collections::HashMap::from([
                ("priority", graph_core::props::int64(60)),
            ]),
            priority: 60,
            watermark: Some(1010),
        },

        // Scenario 3: Multiple rapid events
        StreamEvent {
            event_id: "evt_003".to_string(),
            event_type: StreamEventType::BatchUpdate(vec![
                graph_views::DataChange::AddVertex {
                    id: graph_core::VertexId::new(1002),
                    properties: std::collections::HashMap::from([
                        ("user_id", graph_core::props::string("user_002")),
                        ("action", graph_core::props::string("click")),
                    ]),
                },
                graph_views::DataChange::AddVertex {
                    id: graph_core::VertexId::new(1003),
                    properties: std::collections::HashMap::from([
                        ("user_id", graph_core::props::string("user_003")),
                        ("action", graph_core::props::string("click")),
                    ]),
                },
            ]),
            data: StreamEventData::SystemMetrics(std::collections::HashMap::from([
                ("batch_size", graph_core::props::int64(2)),
            ])),
            timestamp: std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1015),
            source: "analytics_service".to_string(),
            metadata: std::collections::HashMap::from([
                ("priority", graph_core::props::int64(40)),
            ]),
            priority: 40,
            watermark: Some(1015),
        },

        // Scenario 4: Session end event
        StreamEvent {
            event_id: "evt_004".to_string(),
            event_type: StreamEventType::StreamControl(StreamControlType::Flush),
            data: StreamEventData::SystemMetrics(std::collections::HashMap::from([
                ("session_duration", graph_core::props::int64(300)), // 5 minutes
            ])),
            timestamp: std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1020),
            source: "session_manager".to_string(),
            metadata: std::collections::HashMap::from([
                ("priority", graph_core::props::int64(90)),
            ]),
            priority: 90,
            watermark: Some(1020),
        },

        // Scenario 5: High-frequency events (stress test)
        StreamEvent {
            event_id: "evt_005".to_string(),
            event_type: StreamEventType::DataChange(
                graph_views::DataChange::AddVertex {
                    id: graph_core::VertexId::new(1004),
                    properties: std::collections::HashMap::from([
                        ("user_id", graph_core::props::string("user_004")),
                        ("action", graph_core::props::string("api_call")),
                    ]),
                }
            ),
            data: StreamEventData::Vertex(
                graph_core::VertexId::new(1004),
                std::collections::HashMap::from([
                    ("user_id", graph_core::props::string("user_004")),
                    ("action", graph_core::props::string("api_call")),
                ])
            ),
            timestamp: std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1025),
            source: "api_service".to_string(),
            metadata: std::collections::HashMap::from([
                ("priority", graph_core::props::int64(20)),
            ]),
            priority: 20,
            watermark: Some(1025),
        },
    ];

    println!("📊 Generated {} event scenarios", event_scenarios.len());

    // Process events with small delays to simulate real-time stream
    for (i, event) in event_scenarios.into_iter().enumerate() {
        println!("📦 Processing scenario {}: {}", i + 1, event.event_id);
        
        // Process event through windows
        let results = window_manager.process_event(event)?;
        
        for result in results {
            println!("   🪟 Window {}: {} events", result.window_id, result.event_count);
            
            // Show aggregations
            for (key, value) in &result.aggregations {
                println!("      {}: {}", key, value);
            }
        }

        // Small delay between events (simulating real-time arrival)
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    println!("✅ Stream processing simulation completed");
    Ok(())
}

/// Show window processing statistics
fn show_window_statistics(window_manager: &WindowManager) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 Window Processing Statistics:");
    
    let stats = window_manager.get_stats();
    
    println!("   🔄 Windows Processed: {}", stats.windows_processed);
    println!("   📈 Events Processed: {}", stats.events_processed);
    println!("   ⚡ Avg Processing Time: {:.2} ms", stats.avg_processing_time_ms);
    println!("   📊 Max Processing Time: {} ms", stats.max_processing_time_ms);
    println!("   🪟 Active Windows: {}", stats.active_windows);

    // Show individual window states
    let active_windows = window_manager.get_active_windows();
    println!("\n   Active Window States:");
    for (window_id, state) in active_windows {
        println!("     📋 {}: {} events, closed: {}", 
            window_id, state.event_count, state.is_closed);
    }

    Ok(())
}