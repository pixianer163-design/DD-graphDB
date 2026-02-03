# 上层应用示例：实时社交网络分析平台

这个目录包含基于 `graph_database` 构建的上层应用示例，展示如何充分利用项目的各项能力。

## 应用示例概览

### 1. 社交网络分析平台 (`social_network_platform.rs`)

一个完整的社交网络应用，展示如何集成和使用 graph_database 的所有核心功能。

#### 核心特性

**物化视图系统**
- 用户资料查找视图（高频查询，事件驱动刷新）
- 粉丝统计视图（聚合查询，混合刷新策略）
- 影响力排名视图（PageRank 算法，定时刷新）
- 社群发现视图（连通分量算法）
- 好友推荐视图（混合视图，多算法组合）

**增量计算引擎**
- 实时更新影响力分数
- 自动依赖传播（社群发现 → 好友推荐）
- 批量变更处理
- 性能统计追踪

**多级缓存**
- L1/L2/L3 三级缓存架构
- 缓存预热机制
- 命中率统计

**智能查询路由**
- 基于查询模式自动选择最优视图
- 性能预估和路由决策
- 回退机制

#### 使用方法

```bash
# 运行社交平台演示
cargo run --bin social_network_platform --features "views streaming"
```

#### API 示例

```rust
// 创建平台实例
let config = PlatformConfig::default();
let platform = SocialNetworkPlatform::new(config).await?;

// 创建用户
let user = UserProfile {
    id: VertexId::new(1),
    username: "alice".to_string(),
    display_name: "Alice Chen".to_string(),
    bio: "Tech enthusiast".to_string(),
    ..Default::default()
};
platform.create_user(user).await?;

// 建立关系
platform.create_relationship(
    VertexId::new(1),
    VertexId::new(2),
    RelationshipType::Follows,
    None
).await?;

// 查询（自动使用物化视图）
let profile = platform.get_user_profile(VertexId::new(1)).await?;

// 获取影响力排名（增量更新的 PageRank）
let ranking = platform.get_influence_ranking(10).await?;
```

---

### 2. 流式分析引擎 (`streaming_analytics_demo.rs`)

使用 differential dataflow 进行实时流处理的深度示例。

#### 核心特性

**Differential Dataflow 集成**
- 实时 PageRank 更新（流式迭代计算）
- 增量式社群发现
- 实时三角形计数（好友推荐基础）
- 影响力变化追踪

**事件驱动架构**
- 用户添加/删除事件
- 关系建立/解除事件
- 属性更新事件
- 异步事件处理

**实时计算**
- 不动点迭代算法
- 增量更新机制
- 多 worker 并行处理

#### 使用方法

```bash
# 运行流式分析演示
cargo run --bin streaming_analytics_demo --features "views streaming"
```

#### 核心算法

**流式 PageRank**
```rust
fn compute_streaming_pagerank<G: Scope>(
    edges: &Collection<G, Edge>
) -> Collection<G, (VertexId, f64)> {
    // 1. 计算出度
    let out_degrees = edges.map(|e| (e.src, 1)).count();
    
    // 2. 准备边贡献 (src -> (dst, 1/out_degree))
    let edge_contributions = edges
        .map(|e| (e.src, e.dst))
        .join(&out_degrees)
        .map(|(src, (dst, out_deg))| (dst, (src, 1.0 / out_deg as f64)));
    
    // 3. 迭代计算（differential dataflow 自动处理增量）
    initial_ranks.iterate(|ranks| {
        let contributions = edge_contrib
            .join_map(ranks, |_dst, (src, contrib), rank| (*src, *contrib * *rank))
            .reduce(|_src, s, t| {
                let total: f64 = s.iter().map(|(_, c)| *c).sum();
                t.push((total, 1));
            });
        
        contributions.map(|(v, total)| {
            let rank = (1.0 - DAMPING) + DAMPING * total;
            (v, rank)
        })
    })
}
```

**流式社群发现**
```rust
fn compute_streaming_communities<G: Scope>(
    edges: &Collection<G, Edge>
) -> Collection<G, (VertexId, u64)> {
    // 初始：每个顶点是自己的社群
    let initial = vertices.map(|v| (v, v.as_u64()));
    
    // 迭代传播最小社群ID
    initial.iterate(|components| {
        edges_in_scope
            .map(|e| (e.src, e.dst))
            .join_map(&components, |_src, dst, comp| (*dst, *comp))
            .concat(components)
            .reduce(|_v, s, t| {
                let min_comp = s.iter().map(|(_, c)| *c).min().unwrap();
                t.push((min_comp, 1));
            })
    })
}
```

---

### 3. REST API 服务 (`api_service.rs`)

使用 Axum 框架构建的生产级 REST API，展示如何在 Web 服务中使用 graph_database。

#### API 端点

**用户管理**
```
POST /api/users              # 创建用户
GET  /api/users/:id          # 获取用户资料
```

**关系管理**
```
POST /api/relationships      # 建立关系（关注/好友等）
```

**分析查询**
```
GET /api/analytics/influence   # 影响力排名（PageRank）
GET /api/analytics/communities # 社群列表（连通分量）
```

**推荐系统**
```
GET /api/recommendations/:user_id  # 好友推荐
```

**平台管理**
```
GET  /api/stats              # 平台统计
POST /api/batch/import       # 批量导入
GET  /health                 # 健康检查
```

#### 使用方法

```bash
# 启动 API 服务
cargo run --bin api_service --features "views streaming"

# 服务启动后会监听 http://0.0.0.0:3000
```

#### API 调用示例

**创建用户**
```bash
curl -X POST http://localhost:3000/api/users \
  -H "Content-Type: application/json" \
  -d '{
    "username": "alice",
    "display_name": "Alice Chen",
    "bio": "Tech enthusiast"
  }'
```

**建立关系**
```bash
curl -X POST http://localhost:3000/api/relationships \
  -H "Content-Type: application/json" \
  -d '{
    "from_user_id": "1",
    "to_user_id": "2",
    "rel_type": "follows"
  }'
```

**获取影响力排名**
```bash
curl http://localhost:3000/api/analytics/influence
```

**获取好友推荐**
```bash
curl http://localhost:3000/api/recommendations/1
```

---

## 架构设计要点

### 1. 物化视图优先

所有查询都通过物化视图，避免运行时查询规划开销：

```
查询请求 → 查询路由器 → 选择最优视图 → 返回预计算结果
                ↓
           缓存检查 → 命中则直接返回
                ↓
           未命中 → 从物化视图获取 → 更新缓存
```

### 2. 增量更新流程

```
数据变更 → ChangeSet → 增量引擎 → 依赖图
                              ↓
                    拓扑排序确定更新顺序
                              ↓
              受影响视图 ← 增量计算（differential dataflow）
                              ↓
                        更新物化视图 → 多级缓存
                              ↓
                        触发依赖视图更新（递归）
```

### 3. 多级缓存策略

- **L1 (Hot)**：最近使用的查询结果，内存存储
- **L2 (Warm)**：常用查询结果，带过期时间
- **L3 (Cold)**：历史查询结果，磁盘存储

### 4. Differential Dataflow 优势

- **增量计算**：数据变化时只重新计算受影响的部分
- **流式处理**：支持实时数据摄取和连续计算
- **高效 Join**：使用 arrangement 优化多路连接
- **时间戳追踪**：支持复杂的时间逻辑和回溯

---

## 性能优化技巧

### 1. 视图刷新策略选择

| 视图类型 | 推荐策略 | 适用场景 |
|---------|---------|---------|
| 用户查找 | EventDriven (50ms) | 高频点查 |
| 粉丝统计 | Hybrid (30s) | 中等频率聚合 |
| 影响力排名 | FixedInterval (5min) | 低频复杂算法 |
| 好友推荐 | FixedInterval (3min) | 计算密集型 |

### 2. 批量操作

```rust
// 批量导入用户
let users: Vec<UserProfile> = ...;
platform.bulk_import_users(users).await?;  // 批量触发增量更新
```

### 3. 缓存预热

```rust
// 启动时预热常见查询
let warm_patterns = vec![
    QueryPattern::VertexLookup { vertex_ids: popular_users },
    QueryPattern::Aggregation { aggregate_type: "count_by_role".to_string(), ... },
];
query_router.warm_up_cache(warm_patterns)?;
```

### 4. 依赖关系优化

```rust
// 避免过度依赖链，减少级联更新
dep_graph.add_dependency(
    "follower_statistics".to_string(),
    "influence_ranking".to_string(),
    DependencyType::Data,  // 数据依赖，变化时触发更新
);
```

---

## 扩展建议

### 1. 添加新的图算法

在 `graph/algorithms/src/lib.rs` 中添加：

```rust
#[cfg(feature = "streaming")]
pub fn my_algorithm<G: Scope>(
    edges: &Collection<G, Edge>,
) -> Collection<G, MyResult> {
    edges.iterate(|current| {
        // 使用 differential dataflow 操作符
        // join, reduce, group, etc.
    })
}
```

### 2. 自定义视图类型

在 `graph/views/src/view_types.rs` 中扩展：

```rust
pub enum ViewType {
    // ... 现有类型
    Custom { 
        algorithm: String,
        parameters: HashMap<String, PropertyValue>,
    },
}
```

### 3. 添加新的查询模式

在 `graph/views/src/query_router.rs` 中添加：

```rust
pub enum QueryPattern {
    // ... 现有模式
    PathFinding {
        source: VertexId,
        target: VertexId,
        max_length: usize,
    },
}
```

---

## 调试和监控

### 查看增量计算统计

```rust
let state = incremental_engine.get_engine_state();
println!("处理变更: {}", state.changes_processed);
println!("更新视图: {}", state.views_updated);
println!("节省时间: {}ms", state.time_saved_ms);
```

### 查看缓存统计

```rust
let stats = cache_manager.get_stats();
println!("总体命中率: {:.2}%", stats.overall_hit_ratio * 100.0);
println!("L1: {:.2}%, L2: {:.2}%, L3: {:.2}%",
    stats.l1_stats.hit_ratio * 100.0,
    stats.l2_stats.hit_ratio * 100.0,
    stats.l3_stats.hit_ratio * 100.0
);
```

### 追踪视图依赖

```rust
let update_order = dependency_graph.get_update_order();
println!("视图更新顺序: {:?}", update_order);
```

---

## 总结

这些上层应用示例展示了如何充分利用 graph_database 的能力：

1. **物化视图系统**：预计算常见查询，消除运行时开销
2. **增量计算引擎**：基于 differential dataflow 实现实时更新
3. **多级缓存**：分层缓存策略优化查询性能
4. **智能路由**：自动选择最优执行路径
5. **流式处理**：支持实时数据摄取和连续计算

通过这些示例，你可以构建高性能、实时的图分析应用，充分利用 Rust 和 differential dataflow 的性能优势。
