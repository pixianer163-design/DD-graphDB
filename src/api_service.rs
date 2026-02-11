//! 社交网络 REST API 服务
//! 
//! 使用 axum 框架构建，展示如何将 graph_database 集成到 Web 服务中
//! 
//! API 端点设计：
//! - POST /api/users - 创建用户
//! - GET /api/users/:id - 获取用户资料
//! - POST /api/relationships - 建立关系
//! - GET /api/analytics/influence - 获取影响力排名
//! - GET /api/analytics/communities - 获取社群列表
//! - GET /api/recommendations/:user_id - 获取好友推荐
//! - GET /api/stats - 获取平台统计
//! - POST /api/batch/import - 批量导入数据

use axum::{
    routing::{get, post},
    Router,
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error};
use tower_http::trace::TraceLayer;

// 复用社交网络平台代码 (included as a module)
#[path = "social_network_platform.rs"]
mod social_network_platform;
use social_network_platform::{
    SocialNetworkPlatform, PlatformConfig, UserProfile,
    RelationshipType, PlatformStats
};
use graph_core::VertexId;

/// 应用状态
#[derive(Clone)]
pub struct AppState {
    platform: Arc<RwLock<SocialNetworkPlatform>>,
}

/// 创建用户请求
#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub display_name: String,
    pub bio: String,
}

/// 创建用户响应
#[derive(Debug, Serialize)]
pub struct CreateUserResponse {
    pub success: bool,
    pub user_id: String,
    pub message: String,
}

/// 用户资料响应
#[derive(Debug, Serialize)]
pub struct UserProfileResponse {
    pub id: String,
    pub username: String,
    pub display_name: String,
    pub bio: String,
    pub follower_count: u64,
    pub following_count: u64,
}

/// 创建关系请求
#[derive(Debug, Deserialize)]
pub struct CreateRelationshipRequest {
    pub from_user_id: String,
    pub to_user_id: String,
    pub rel_type: String,
}

/// 影响力排名响应
#[derive(Debug, Serialize)]
pub struct InfluenceRankingResponse {
    pub rankings: Vec<InfluenceRankingItem>,
    pub total: usize,
    pub computed_at: String,
}

#[derive(Debug, Serialize)]
pub struct InfluenceRankingItem {
    pub user_id: String,
    pub username: String,
    pub influence_score: f64,
    pub rank: usize,
}

/// 社群响应
#[derive(Debug, Serialize)]
pub struct CommunitiesResponse {
    pub communities: Vec<CommunityItem>,
    pub total_communities: usize,
}

#[derive(Debug, Serialize)]
pub struct CommunityItem {
    pub community_id: u64,
    pub member_count: usize,
    pub density: f64,
    pub cohesion_score: f64,
    pub sample_members: Vec<String>,
}

/// 好友推荐响应
#[derive(Debug, Serialize)]
pub struct RecommendationsResponse {
    pub user_id: String,
    pub recommendations: Vec<RecommendationItem>,
}

#[derive(Debug, Serialize)]
pub struct RecommendationItem {
    pub user_id: String,
    pub username: String,
    pub score: f64,
    pub reason: String,
}

/// 平台统计响应
#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub user_count: usize,
    pub relationship_count: usize,
    pub changes_processed: u64,
    pub views_updated: u64,
    pub cache_hit_ratio: f64,
    pub time_saved_ms: u64,
}

/// 批量导入请求
#[derive(Debug, Deserialize)]
pub struct BulkImportRequest {
    pub users: Vec<CreateUserRequest>,
}

/// 批量导入响应
#[derive(Debug, Serialize)]
pub struct BulkImportResponse {
    pub success: bool,
    pub imported_count: usize,
    pub failed_count: usize,
    pub message: String,
}

/// 错误响应
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

/// 创建 API 路由
pub fn create_router(platform: Arc<RwLock<SocialNetworkPlatform>>) -> Router {
    let state = AppState { platform };
    
    Router::new()
        // 用户管理
        .route("/api/users", post(create_user))
        .route("/api/users/:id", get(get_user))
        
        // 关系管理
        .route("/api/relationships", post(create_relationship))
        
        // 分析查询
        .route("/api/analytics/influence", get(get_influence_ranking))
        .route("/api/analytics/communities", get(get_communities))
        
        // 推荐系统
        .route("/api/recommendations/:user_id", get(get_recommendations))
        
        // 平台管理
        .route("/api/stats", get(get_stats))
        .route("/api/batch/import", post(bulk_import))
        
        // 健康检查
        .route("/health", get(health_check))
        
        // 添加中间件
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// 健康检查
async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "social-network-api",
        "version": "1.0.0"
    }))
}

/// 创建用户
async fn create_user(
    State(state): State<AppState>,
    Json(req): Json<CreateUserRequest>,
) -> impl IntoResponse {
    info!("📥 创建用户请求: {}", req.username);
    
    let platform = state.platform.read().await;
    
    // 生成用户ID
    let user_id = VertexId::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    
    let profile = UserProfile {
        id: user_id,
        username: req.username.clone(),
        display_name: req.display_name,
        bio: req.bio,
        follower_count: 0,
        following_count: 0,
        join_timestamp: chrono::Utc::now().timestamp(),
    };
    
    match platform.create_user(profile).await {
        Ok(id) => {
            info!("✅ 用户创建成功: {:?}", id);
            (
                StatusCode::CREATED,
                Json(CreateUserResponse {
                    success: true,
                    user_id: format!("{:?}", id),
                    message: "User created successfully".to_string(),
                })
            )
        }
        Err(e) => {
            error!("❌ 创建用户失败: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CreateUserResponse {
                    success: false,
                    user_id: "".to_string(),
                    message: format!("Failed to create user: {}", e),
                })
            )
        }
    }
}

/// 获取用户资料
async fn get_user(
    State(state): State<AppState>,
    Path(user_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    info!("🔍 查询用户: {}", user_id);

    // 解析用户ID
    let vertex_id = match user_id.parse::<u64>() {
        Ok(id) => VertexId::new(id),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "Invalid user ID", "message": "User ID must be a valid u64"})),
            );
        }
    };

    let platform = state.platform.read().await;

    match platform.get_user_profile(vertex_id).await {
        Ok(Some(profile)) => {
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "id": format!("{:?}", profile.id),
                    "username": profile.username,
                    "display_name": profile.display_name,
                    "bio": profile.bio,
                    "follower_count": profile.follower_count,
                    "following_count": profile.following_count,
                })),
            )
        }
        Ok(None) => {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "User not found", "message": format!("No user found with ID: {}", user_id)})),
            )
        }
        Err(e) => {
            error!("❌ 查询用户失败: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Query failed", "message": format!("Failed to get user: {}", e)})),
            )
        }
    }
}

/// 创建关系
async fn create_relationship(
    State(state): State<AppState>,
    Json(req): Json<CreateRelationshipRequest>,
) -> impl IntoResponse {
    info!("🔗 创建关系: {} -> {}", req.from_user_id, req.to_user_id);
    
    // 解析用户ID
    let from_id = match req.from_user_id.parse::<u64>() {
        Ok(id) => VertexId::new(id),
        Err(_) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "Invalid from_user_id"}))),
    };
    
    let to_id = match req.to_user_id.parse::<u64>() {
        Ok(id) => VertexId::new(id),
        Err(_) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "Invalid to_user_id"}))),
    };
    
    // 解析关系类型
    let rel_type = match req.rel_type.as_str() {
        "follows" => RelationshipType::Follows,
        "friends" => RelationshipType::Friends,
        "blocks" => RelationshipType::Blocks,
        "mentions" => RelationshipType::Mentions,
        _ => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "Invalid relationship type"}))),
    };
    
    let platform = state.platform.read().await;
    
    match platform.create_relationship(from_id, to_id, rel_type, None).await {
        Ok(_) => {
            info!("✅ 关系创建成功");
            (StatusCode::CREATED, Json(serde_json::json!({"success": true, "message": "Relationship created"})))
        }
        Err(e) => {
            error!("❌ 创建关系失败: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))
        }
    }
}

/// 获取影响力排名
async fn get_influence_ranking(
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("📊 获取影响力排名");
    
    let platform = state.platform.read().await;
    
    match platform.get_influence_ranking(100).await {
        Ok(ranking) => {
            let items: Vec<InfluenceRankingItem> = ranking
                .into_iter()
                .map(|r| InfluenceRankingItem {
                    user_id: format!("{:?}", r.user_id),
                    username: format!("user_{:?}", r.user_id), // 简化处理
                    influence_score: r.influence_score,
                    rank: r.rank,
                })
                .collect();
            
            let total = items.len();
            (
                StatusCode::OK,
                Json(InfluenceRankingResponse {
                    rankings: items,
                    total,
                    computed_at: chrono::Utc::now().to_rfc3339(),
                })
            )
        }
        Err(e) => {
            error!("❌ 获取影响力排名失败: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(InfluenceRankingResponse {
                    rankings: vec![],
                    total: 0,
                    computed_at: chrono::Utc::now().to_rfc3339(),
                })
            )
        }
    }
}

/// 获取社群列表
async fn get_communities(
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("👥 获取社群列表");
    
    let platform = state.platform.read().await;
    
    match platform.get_communities().await {
        Ok(communities) => {
            let items: Vec<CommunityItem> = communities
                .into_iter()
                .map(|c| CommunityItem {
                    community_id: c.community_id,
                    member_count: c.members.len(),
                    density: c.density,
                    cohesion_score: c.cohesion_score,
                    sample_members: c.members.iter()
                        .take(5)
                        .map(|m| format!("{:?}", m))
                        .collect(),
                })
                .collect();
            
            let total = items.len();
            (
                StatusCode::OK,
                Json(CommunitiesResponse {
                    communities: items,
                    total_communities: total,
                })
            )
        }
        Err(e) => {
            error!("❌ 获取社群列表失败: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CommunitiesResponse {
                    communities: vec![],
                    total_communities: 0,
                })
            )
        }
    }
}

/// 获取好友推荐
async fn get_recommendations(
    State(state): State<AppState>,
    Path(user_id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    info!("🎯 获取好友推荐: {}", user_id);

    let vertex_id = match user_id.parse::<u64>() {
        Ok(id) => VertexId::new(id),
        Err(_) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "Invalid user ID"}))),
    };

    let platform = state.platform.read().await;

    match platform.get_friend_recommendations(vertex_id, 10).await {
        Ok(recommendations) => {
            let items: Vec<serde_json::Value> = recommendations
                .into_iter()
                .map(|(id, score)| serde_json::json!({
                    "user_id": format!("{:?}", id),
                    "username": format!("user_{:?}", id),
                    "score": score,
                    "reason": "Common connections",
                }))
                .collect();

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "user_id": user_id,
                    "recommendations": items,
                })),
            )
        }
        Err(e) => {
            error!("❌ 获取推荐失败: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "user_id": user_id,
                    "recommendations": [],
                })),
            )
        }
    }
}

/// 获取平台统计
async fn get_stats(
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("📈 获取平台统计");
    
    let platform = state.platform.read().await;
    
    match platform.get_platform_stats().await {
        Ok(stats) => {
            (
                StatusCode::OK,
                Json(StatsResponse {
                    user_count: stats.user_count,
                    relationship_count: stats.relationship_count,
                    changes_processed: stats.changes_processed,
                    views_updated: stats.views_updated,
                    cache_hit_ratio: stats.cache_hit_ratio,
                    time_saved_ms: stats.time_saved_ms,
                })
            )
        }
        Err(e) => {
            error!("❌ 获取统计失败: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(StatsResponse {
                    user_count: 0,
                    relationship_count: 0,
                    changes_processed: 0,
                    views_updated: 0,
                    cache_hit_ratio: 0.0,
                    time_saved_ms: 0,
                })
            )
        }
    }
}

/// 批量导入
async fn bulk_import(
    State(state): State<AppState>,
    Json(req): Json<BulkImportRequest>,
) -> impl IntoResponse {
    let user_count = req.users.len();
    info!("📥 批量导入请求: {} 个用户", user_count);

    let platform = state.platform.read().await;

    // 转换请求为用户资料
    let profiles: Vec<UserProfile> = req.users.into_iter().enumerate().map(|(i, u)| {
        UserProfile {
            id: VertexId::new(i as u64 + 1),
            username: u.username,
            display_name: u.display_name,
            bio: u.bio,
            follower_count: 0,
            following_count: 0,
            join_timestamp: chrono::Utc::now().timestamp(),
        }
    }).collect();

    match platform.bulk_import_users(profiles).await {
        Ok(count) => {
            info!("✅ 批量导入成功: {} 个用户", count);
            (
                StatusCode::OK,
                Json(BulkImportResponse {
                    success: true,
                    imported_count: count,
                    failed_count: 0,
                    message: format!("Successfully imported {} users", count),
                }),
            )
        }
        Err(e) => {
            error!("❌ 批量导入失败: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BulkImportResponse {
                    success: false,
                    imported_count: 0,
                    failed_count: user_count,
                    message: format!("Import failed: {}", e),
                }),
            )
        }
    }
}

/// 启动 API 服务器
pub async fn start_api_server(
    platform: SocialNetworkPlatform,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::net::TcpListener;
    
    info!("🚀 启动社交网络 API 服务...");
    
    let platform = Arc::new(RwLock::new(platform));
    let app = create_router(platform);
    
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    
    info!("✅ API 服务已启动，监听: http://{}", addr);
    info!("📚 API 文档:");
    info!("   POST /api/users - 创建用户");
    info!("   GET  /api/users/:id - 获取用户资料");
    info!("   POST /api/relationships - 建立关系");
    info!("   GET  /api/analytics/influence - 影响力排名");
    info!("   GET  /api/analytics/communities - 社群列表");
    info!("   GET  /api/recommendations/:user_id - 好友推荐");
    info!("   GET  /api/stats - 平台统计");
    info!("   POST /api/batch/import - 批量导入");
    info!("   GET  /health - 健康检查");
    
    axum::serve(listener, app).await?;
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 创建平台实例
    let config = PlatformConfig::default();
    let platform = SocialNetworkPlatform::new(config).await?;
    
    // 启动 API 服务
    start_api_server(platform, 3000).await?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;
    
    #[tokio::test]
    async fn test_health_check() {
        // 这是一个简单的测试示例
        // 实际测试需要 mock platform
        
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        
        // 这里需要创建 router 并测试
        // 简化示例，实际需要完整初始化
    }
}
