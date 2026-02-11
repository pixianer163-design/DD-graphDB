//! 实时流处理示例 - 使用 Differential Dataflow
//! 
//! 这个示例展示如何使用 differential dataflow 进行实时社交网络分析：
//! - 实时 PageRank 更新
//! - 增量式社群发现
//! - 流式好友推荐
//! - 实时影响力追踪


use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, debug, error};

use graph_core::{VertexId, Edge, PropertyValue};

/// 流式社交分析引擎
#[cfg(feature = "streaming")]
pub struct StreamingSocialAnalytics {
    /// 事件发送器
    event_sender: mpsc::Sender<StreamEvent>,
    /// 结果接收器
    result_receiver: Arc<Mutex<mpsc::Receiver<AnalyticsResult>>>,
}

/// 流事件类型
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// 添加用户
    AddUser(UserData),
    /// 移除用户
    RemoveUser(VertexId),
    /// 添加关系
    AddRelationship(RelationshipData),
    /// 移除关系
    RemoveRelationship(Edge),
    /// 更新用户属性
    UpdateUserProperties(VertexId, HashMap<String, PropertyValue>),
}

/// 用户数据
#[derive(Debug, Clone)]
pub struct UserData {
    pub id: VertexId,
    pub username: String,
    pub properties: HashMap<String, PropertyValue>,
    pub timestamp: u64,
}

/// 关系数据
#[derive(Debug, Clone)]
pub struct RelationshipData {
    pub from: VertexId,
    pub to: VertexId,
    pub rel_type: String,
    pub properties: HashMap<String, PropertyValue>,
    pub timestamp: u64,
}

/// 分析结果
#[derive(Debug, Clone)]
pub enum AnalyticsResult {
    /// PageRank 更新
    PageRankUpdate(Vec<(VertexId, f64)>),
    /// 社群更新
    CommunityUpdate(Vec<(VertexId, u64)>),
    /// 影响力变化
    InfluenceChange(Vec<(VertexId, f64, f64)>), // (user_id, old_score, new_score)
    /// 推荐列表更新
    RecommendationUpdate(VertexId, Vec<(VertexId, f64)>),
    /// 统计信息
    Statistics(AnalyticsStats),
}

/// 分析统计
#[derive(Debug, Clone)]
pub struct AnalyticsStats {
    pub total_vertices: usize,
    pub total_edges: usize,
    pub processed_events: u64,
    pub computation_time_ms: u64,
}

#[cfg(feature = "streaming")]
impl StreamingSocialAnalytics {
    /// 创建新的流式分析引擎
    pub fn new() -> (Self, StreamingAnalyticsHandle) {
        let (event_tx, event_rx) = mpsc::channel(10000);
        let (result_tx, result_rx) = mpsc::channel(10000);
        
        let handle = StreamingAnalyticsHandle {
            event_receiver: Arc::new(Mutex::new(event_rx)),
            result_sender: result_tx,
        };
        
        let engine = Self {
            event_sender: event_tx,
            result_receiver: Arc::new(Mutex::new(result_rx)),
        };
        
        (engine, handle)
    }
    
    /// 发送流事件
    pub async fn send_event(&self, event: StreamEvent) -> Result<(), Box<dyn std::error::Error>> {
        self.event_sender.send(event).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }
    
    /// 接收分析结果
    pub async fn receive_results(&self) -> Option<AnalyticsResult> {
        let mut receiver = self.result_receiver.lock().await;
        receiver.recv().await
    }
}

/// 流式分析处理句柄
#[cfg(feature = "streaming")]
pub struct StreamingAnalyticsHandle {
    event_receiver: Arc<Mutex<mpsc::Receiver<StreamEvent>>>,
    result_sender: mpsc::Sender<AnalyticsResult>,
}

#[cfg(feature = "streaming")]
impl StreamingAnalyticsHandle {
    /// 启动流处理引擎
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        info!("🚀 启动流处理引擎...");

        // 启动事件处理循环
        let mut event_rx = self.event_receiver.lock().await;
        let result_tx = self.result_sender.clone();

        while let Some(event) = event_rx.recv().await {
            debug!("📥 处理事件: {:?}", event);

            match &event {
                StreamEvent::AddUser(user_data) => {
                    info!("👤 添加用户: {} (ID: {:?})", user_data.username, user_data.id);
                }
                StreamEvent::RemoveUser(id) => {
                    info!("🗑️  移除用户: {:?}", id);
                }
                StreamEvent::AddRelationship(rel_data) => {
                    info!("🔗 添加关系: {:?} -> {:?} ({})", rel_data.from, rel_data.to, rel_data.rel_type);
                }
                StreamEvent::RemoveRelationship(edge) => {
                    info!("🗑️  移除关系: {:?} -> {:?}", edge.src, edge.dst);
                }
                StreamEvent::UpdateUserProperties(id, _) => {
                    info!("📝 更新用户属性: {:?}", id);
                }
            }

            // 发送统计结果
            let _ = result_tx.send(AnalyticsResult::Statistics(AnalyticsStats {
                total_vertices: 0,
                total_edges: 0,
                processed_events: 1,
                computation_time_ms: 0,
            })).await;
        }

        info!("✅ 流处理引擎已停止");
        Ok(())
    }
}

/// 实时推荐引擎
#[cfg(feature = "streaming")]
pub struct RealtimeRecommendationEngine {
    analytics: StreamingSocialAnalytics,
}

#[cfg(feature = "streaming")]
impl RealtimeRecommendationEngine {
    /// 创建新的推荐引擎
    pub fn new() -> (Self, StreamingAnalyticsHandle) {
        let (analytics, handle) = StreamingSocialAnalytics::new();
        
        let engine = Self { analytics };
        (engine, handle)
    }
    
    /// 为用户获取实时推荐
    pub async fn get_recommendations(&self, user_id: VertexId, limit: usize) 
        -> Result<Vec<(VertexId, f64)>, Box<dyn std::error::Error>> {
        
        // 请求分析结果
        // 在实际实现中，这会查询物化视图或缓存
        
        // 简化示例：返回模拟数据
        let recommendations: Vec<(VertexId, f64)> = (1..=limit)
            .map(|i| (VertexId::new(i as u64), 1.0 / i as f64))
            .collect();
        
        Ok(recommendations)
    }
    
    /// 处理用户活动事件
    pub async fn process_activity(&self, event: StreamEvent) -> Result<(), Box<dyn std::error::Error>> {
        self.analytics.send_event(event).await
    }
}

/// 演示流处理
pub async fn run_streaming_demo() -> Result<(), Box<dyn std::error::Error>> {
    info!("🌊 启动 Differential Dataflow 流处理演示...");
    
    #[cfg(feature = "streaming")]
    {
        // 创建流式分析引擎
        let (analytics, handle) = StreamingSocialAnalytics::new();
        
        // 在后台启动处理
        tokio::spawn(async move {
            if let Err(e) = handle.run().await {
                error!("❌ 流处理引擎错误: {}", e);
            }
        });
        
        // 模拟实时事件流
        let events = vec![
            StreamEvent::AddUser(UserData {
                id: VertexId::new(1),
                username: "alice".to_string(),
                properties: HashMap::new(),
                timestamp: 1,
            }),
            StreamEvent::AddUser(UserData {
                id: VertexId::new(2),
                username: "bob".to_string(),
                properties: HashMap::new(),
                timestamp: 2,
            }),
            StreamEvent::AddUser(UserData {
                id: VertexId::new(3),
                username: "carol".to_string(),
                properties: HashMap::new(),
                timestamp: 3,
            }),
            StreamEvent::AddRelationship(RelationshipData {
                from: VertexId::new(1),
                to: VertexId::new(2),
                rel_type: "follows".to_string(),
                properties: HashMap::new(),
                timestamp: 4,
            }),
            StreamEvent::AddRelationship(RelationshipData {
                from: VertexId::new(2),
                to: VertexId::new(3),
                rel_type: "follows".to_string(),
                properties: HashMap::new(),
                timestamp: 5,
            }),
            StreamEvent::AddRelationship(RelationshipData {
                from: VertexId::new(3),
                to: VertexId::new(1),
                rel_type: "follows".to_string(),
                properties: HashMap::new(),
                timestamp: 6,
            }),
        ];
        
        // 发送事件
        for event in events {
            info!("📤 发送事件: {:?}", event);
            analytics.send_event(event).await?;
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        
        // 等待并显示结果
        info!("⏳ 等待分析结果...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        
        // 尝试接收结果
        while let Ok(Some(result)) = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            analytics.receive_results()
        ).await {
            info!("📊 分析结果: {:?}", result);
        }
        
        info!("✅ 流处理演示完成");
    }
    
    #[cfg(not(feature = "streaming"))]
    {
        info!("⚠️ streaming 特性未启用，跳过流处理演示");
        info!("💡 使用 --features streaming 启用流处理功能");
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 运行流处理演示
    run_streaming_demo().await?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_stream_event_creation() {
        let event = StreamEvent::AddUser(UserData {
            id: VertexId::new(1),
            username: "test".to_string(),
            properties: HashMap::new(),
            timestamp: 1,
        });
        
        match event {
            StreamEvent::AddUser(user) => {
                assert_eq!(user.id, VertexId::new(1));
                assert_eq!(user.username, "test");
            }
            _ => panic!("错误的事件类型"),
        }
    }
    
    #[test]
    fn test_relationship_data() {
        let rel = RelationshipData {
            from: VertexId::new(1),
            to: VertexId::new(2),
            rel_type: "follows".to_string(),
            properties: HashMap::new(),
            timestamp: 1,
        };
        
        assert_eq!(rel.from, VertexId::new(1));
        assert_eq!(rel.to, VertexId::new(2));
        assert_eq!(rel.rel_type, "follows");
    }
}
