//! 实时流处理示例 - 使用 Differential Dataflow
//! 
//! 这个示例展示如何使用 differential dataflow 进行实时社交网络分析：
//! - 实时 PageRank 更新
//! - 增量式社群发现
//! - 流式好友推荐
//! - 实时影响力追踪

#[cfg(feature = "streaming")]
use differential_dataflow::{
    collection::Collection,
    input::InputSession,
    operators::{arrange::Arrange, iterate::Iterate},
    AsCollection,
};

#[cfg(feature = "streaming")]
use timely::{
    dataflow::Scope,
    communication::Allocate,
    worker::Worker,
};

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
    /// 启动 differential dataflow 处理
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        info!("🚀 启动 Differential Dataflow 流处理引擎...");
        
        // 启动 timely 计算
        timely::execute_from_args(std::env::args(), move |worker| {
            info!("👷 启动 Worker {}", worker.index());
            
            // 创建数据流作用域
            worker.dataflow::<usize, _, _>(|scope| {
                // 创建输入会话
                let mut vertices_input: InputSession<usize, (VertexId, HashMap<String, PropertyValue>), isize> = 
                    InputSession::new();
                let mut edges_input: InputSession<usize, Edge, isize> = 
                    InputSession::new();
                
                // 构建顶点集合
                let vertices = vertices_input.to_collection(scope);
                
                // 构建边集合
                let edges = edges_input.to_collection(scope);
                
                // 1. 实时 PageRank 计算
                let pagerank = Self::compute_streaming_pagerank(&edges);
                
                // 2. 实时连通分量（社群发现）
                let communities = Self::compute_streaming_communities(&edges);
                
                // 3. 实时三角形计数（用于好友推荐）
                let triangles = Self::compute_streaming_triangles(&edges);
                
                // 4. 实时监控和统计
                let stats = vertices.count().join(&edges.count());
                
                // 在实际应用中，这里会将结果输出到下游系统
                // 这里简化处理，只打印日志
                pagerank.inspect(|(v, rank)| {
                    debug!("PageRank: {:?} = {:.6}", v, rank);
                });
                
                communities.inspect(|(v, community)| {
                    debug!("Community: {:?} -> {}", v, community);
                });
                
                // 返回输入会话以便外部控制
                (vertices_input, edges_input)
            });
            
            // 在实际实现中，这里会有一个循环从 channel 接收事件并更新输入
            // 简化示例：
            info!("✅ Worker {} 数据流构建完成", worker.index());
        }).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        
        Ok(())
    }
    
    /// 流式 PageRank 计算
    fn compute_streaming_pagerank<G: Scope>(
        edges: &Collection<G, Edge>
    ) -> Collection<G, (VertexId, f64)>
    where
        G::Timestamp: differential_dataflow::lattice::Lattice,
    {
        use differential_dataflow::operators::{Join, Reduce, Threshold};
        
        // 获取所有顶点
        let vertices = edges
            .map(|edge| edge.src)
            .concat(&edges.map(|edge| edge.dst))
            .distinct();
        
        // 初始排名：每个顶点为 1.0
        let initial_ranks = vertices.map(|v| (v, 1.0));
        
        // 计算出度
        let out_degrees = edges
            .map(|edge| (edge.src, 1isize))
            .reduce(|_src, s, t| {
                let sum: isize = s.iter().map(|(_, d)| *d).sum();
                t.push((sum, 1));
            });
        
        // 准备边贡献
        let edge_contributions = edges
            .map(|edge| (edge.src, edge.dst))
            .join(&out_degrees)
            .map(|(src, (dst, out_deg))| {
                (dst, (src, 1.0 / out_deg as f64))
            });
        
        // 迭代计算 PageRank
        let damping_factor = 0.85;
        
        initial_ranks.iterate(|ranks| {
            let edge_contrib = edge_contributions.enter(&ranks.scope());
            
            // 计算贡献
            let contributions = edge_contrib
                .join_map(ranks, |_dst, (src, contrib), rank| {
                    (*src, *contrib * *rank)
                });
            
            // 聚合贡献
            let new_ranks = contributions
                .reduce(|_src, s, t| {
                    let total: f64 = s.iter().map(|(_, contrib)| *contrib).sum();
                    t.push((total, 1));
                })
                .map(|(v, total)| {
                    // 应用阻尼因子
                    let rank = (1.0 - damping_factor) + damping_factor * total;
                    (v, rank)
                });
            
            new_ranks
        })
    }
    
    /// 流式社群发现（连通分量）
    fn compute_streaming_communities<G: Scope>(
        edges: &Collection<G, Edge>
    ) -> Collection<G, (VertexId, u64)>
    where
        G::Timestamp: differential_dataflow::lattice::Lattice,
    {
        use differential_dataflow::operators::{Join, Reduce};
        
        // 获取所有顶点
        let vertices = edges
            .map(|edge| edge.src)
            .concat(&edges.map(|edge| edge.dst))
            .distinct();
        
        // 初始：每个顶点是自己的社群（用顶点ID作为社群ID）
        let initial_components = vertices.map(|v| (v, v.as_u64()));
        
        // 迭代找到最小连通分量
        initial_components.iterate(|components| {
            let edges_in_scope = edges.enter(&components.scope());
            let components_in_scope = components.enter(&components.scope());
            
            // 通过边传播最小社群ID
            let propagated = edges_in_scope
                .map(|edge| (edge.src, edge.dst))
                .join_map(&components_in_scope, |_src, dst, comp| {
                    (*dst, *comp)
                })
                .concat(components_in_scope);
            
            // 为每个顶点选择最小的社群ID
            propagated
                .reduce(|_v, s, t| {
                    let min_comp = s.iter().map(|(_, comp)| *comp).min().unwrap();
                    t.push((min_comp, 1));
                })
                .map(|(v, comp)| (v, comp))
        })
    }
    
    /// 流式三角形计数（用于共同好友推荐）
    fn compute_streaming_triangles<G: Scope>(
        edges: &Collection<G, Edge>
    ) -> Collection<G, (VertexId, VertexId, VertexId)>
    where
        G::Timestamp: differential_dataflow::lattice::Lattice,
    {
        use differential_dataflow::operators::Join;
        
        // 将边视为无向（添加反向边）
        let undirected_edges = edges
            .concat(&edges.map(|edge| Edge::new(edge.dst, edge.src, &edge.label)));
        
        // 查找三角形：如果 A-B 和 B-C，检查 A-C
        let edges_forward = undirected_edges.map(|edge| (edge.src, edge.dst));
        let edges_reverse = undirected_edges.map(|edge| (edge.dst, edge.src));
        
        // 通过连接找到三角形
        edges_forward
            .join(&edges_forward)
            .map(|(b, (a, c))| (a, b, c))
            .filter(|(a, b, c)| a < b && b < c) // 规范化，避免重复
    }
    
    /// 计算实时影响力变化
    fn compute_influence_changes<G: Scope>(
        current_ranks: &Collection<G, (VertexId, f64)>,
        previous_ranks: &Collection<G, (VertexId, f64)>,
    ) -> Collection<G, (VertexId, f64, f64)>
    where
        G::Timestamp: differential_dataflow::lattice::Lattice,
    {
        use differential_dataflow::operators::Join;
        
        // 连接当前和之前的排名，计算变化
        current_ranks
            .join(previous_ranks)
            .map(|(v, (current, previous))| (v, previous, current))
            .filter(|(_, previous, current)| (current - previous).abs() > 0.001)
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
