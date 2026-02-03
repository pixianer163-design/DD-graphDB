# 📋 图数据库技术规范

## 🎯 1. 核心类型系统

### VertexId - 节点标识符
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct VertexId(pub u64);
```

**技术规格：**
- **数据类型**: 64位无符号整数
- **唯一性保证**: 理论上18.4 quintillion个节点
- **哈希优化**: FNV-1a或默认Hash算法
- **内存对齐**: 8字节对齐，缓存友好
- **序列化**: 支持bincode, serde JSON

**性能特征：**
- **O(1)比较**: 整数直接比较
- **O(1)哈希**: 标准整数哈希
- **Copy语义**: 零成本复制
- **栈分配**: 避免堆内存分配

### Edge - 图边结构
```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Edge {
    pub src: VertexId,
    pub dst: VertexId,
    pub label: String,
}
```

**设计考量：**
- **有向性**: 明确区分源和目标
- **自环允许**: src == dst的边有效
- **标签分离**: label与节点ID分离存储
- **哈希组合**: (src, dst, label)唯一哈希

### PropertyValue - 属性值系统
```rust
#[derive(Debug, Clone, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PropertyValue {
    String(String),        // UTF-8字符串
    Int64(i64),          // 64位有符号整数
    Float64(f64),        // IEEE-754双精度
    Bool(bool),           // 布尔值
    Vec(Vec<PropertyValue>), // 嵌套数组
    Null,                // 空值
}
```

**内存布局：**
- **枚举判别**: 8字节tag
- **字符串变体**: 24字节(指针+len+cap)
- **整数变体**: 8字节(tag+value)
- **浮点变体**: 8字节(tag+value)
- **布尔变体**: 1字节(tag+value)
- **数组变体**: 24字节(指针+len+cap)
- **空值变体**: 0字节(仅tag)

---

## 💾 2. 存储引擎规范

### WAL - Write-Ahead Log

**文件结构：**
```
graph.wal
├── [操作1长度: u64][操作1数据: bytes]
├── [操作2长度: u64][操作2数据: bytes]
└── ...
```

**操作序列化：**
```rust
// 使用bincode序列化
let serialized = bincode::serialize(&operation)?;
// 写入长度前缀
wal.write_all(&(serialized.len() as u64).to_le_bytes())?;
wal.write_all(&serialized)?;
```

**同步策略：**
- **批量阈值**: 100个操作或5秒超时
- **强制同步**: fsync::All保证持久化
- **错误处理**: 自动重试和故障恢复

### 快照管理

**快照格式：**
```
graph.snap
├── version: u64
├── timestamp: SystemTime
├── vertex_count: u64
├── vertices: [vertex_data...]
└── edges: [edge_data...]
```

**压缩策略：**
- **LZ4压缩**: 快照数据压缩存储
- **增量快照**: 仅保存差异变化
- **版本链**: 支持时间点查询
- **清理策略**: 保留最近10个版本

---

## 🔍 3. 查询处理规范

### GQL语法

**匹配模式：**
```
MATCH (v:Person {name: 'Alice'})-[r:friend]->(u:Person)
WHERE v.age > 25 AND u.location = 'Beijing'
RETURN v.name, u.name, r.since
```

**创建模式：**
```
CREATE (p:Person {name: 'Bob', age: 30})
CREATE (p)-[w:works_at]->(c:Company {name: 'TechCorp'})
```

**删除模式：**
```
MATCH (p:Person {name: 'Alice'}) 
DETACH DELETE p
```

### AST结构

```rust
pub enum Statement {
    Match {
        pattern: GraphPattern,
        where_clause: Option<Expression>,
        return_items: Vec<ReturnItem>,
    },
    Create { pattern: GraphPattern },
    Delete { variable: String },
}
```

### 表达式系统

**运算符优先级：**
1. **比较运算**: =, !=, >, <, >=, <=
2. **逻辑运算**: AND, OR, NOT
3. **算术运算**: +, -, *, /
4. **函数调用**: lower(), upper(), count()

**优化策略：**
- **谓词下推**: WHERE条件尽早执行
- **索引利用**: 属性索引快速查找
- **连接重排**: 小表驱动大表连接

---

## 📈 4. 算法实现规范

### 差分数据流

**数据流表示：**
```rust
// 时间戳化的集合
Collection<G, (VertexId, Properties)>
```

**更新语义：**
- **Insert**: (data, +1) - 插入新数据
- **Remove**: (data, -1) - 删除已有数据
- **Retract**: 撤销之前的插入
- **Progress**: 时间戳推进

### 核心算法

#### 可达性算法
```rust
// Floyd-Warshall的增量版本
initial_reach = all_edges.distinct()
reach = initial_reach.iterate(|current| {
    let new_reach = current.join(&edges).concat(&current);
    new_reach.distinct()
});
```

**性能指标：**
- **时间复杂度**: O(|V| * |E| * log T)
- **空间复杂度**: O(|V|²)
- **增量更新**: O(ΔE * log T)

#### PageRank算法
```rust
// 幂迭代法的流式实现
ranks = vertices.map(|v| (v, 1.0 / n));
ranks.iterate(|current_ranks| {
    let contributions = edges.join(&current_ranks)
        .map(|(_, (edge, rank))| (edge.dst, rank / out_degree(edge.src)));
    contributions.reduce(|_, contribs| {
        let sum: f64 = contribs.iter().map(|(_, c)| *c).sum();
        (damping_factor * sum + (1.0 - damping_factor))
    })
});
```

**收敛条件：**
- **L1范数**: ||ranks_new - ranks_old||₁ < 1e-6
- **最大迭代**: 100次迭代保证收敛
- **阻尼因子**: 典型值0.85

---

## 🌐 5. 服务接口规范

### HTTP REST API

**端点设计：**

#### 节点操作
```
GET    /vertices              # 列出所有节点
GET    /vertices/{id}         # 获取指定节点
POST   /vertices              # 创建新节点
PUT    /vertices/{id}         # 更新节点
DELETE /vertices/{id}         # 删除节点
```

#### 边操作
```
GET    /edges                 # 列出所有边
GET    /edges/{src}/{dst}    # 获取指定边
POST   /edges                 # 创建新边
PUT    /edges/{src}/{dst}    # 更新边
DELETE /edges/{src}/{dst}    # 删除边
```

#### 查询操作
```
POST   /query                 # 执行GQL查询
GET    /query/stats           # 查询性能统计
POST   /query/explain         # 查询计划解释
```

**响应格式：**
```json
{
    "status": "success|error",
    "data": {...} | null,
    "error": {
        "code": "INVALID_QUERY",
        "message": "Syntax error at line 3"
    } | null,
    "metadata": {
        "execution_time_ms": 45,
        "rows_returned": 127,
        "bytes_read": 2048
    }
}
```

### gRPC服务

**Proto定义：**
```protobuf
service GraphDatabase {
    rpc ExecuteQuery(QueryRequest) returns (QueryResponse);
    rpc CreateVertex(VertexRequest) returns (VertexResponse);
    rpc CreateEdge(EdgeRequest) returns (EdgeResponse);
    rpc GetVertex(VertexIdRequest) returns (VertexResponse);
    rpc StreamUpdates(UpdateRequest) returns (stream UpdateResponse);
}
```

**流式特性：**
- **双向流**: 客户端和服务端都能发送
- **背压处理**: 流量控制和缓冲
- **错误处理**: 流级别错误传播

---

## ⚙️ 6. 配置管理

### 配置文件格式

```toml
[server]
host = "0.0.0.0"
port = 50051
workers = 4

[storage]
data_dir = "./graph_data"
wal_sync_interval = 100
checkpoint_interval = 1000
max_snapshots = 10

[query]
timeout_ms = 30000
max_result_size = 10000
cache_size_mb = 256

[logging]
level = "info"
file = "./logs/graph.log"
max_file_size_mb = 100
max_files = 5
```

### 环境变量

```bash
export GRAPH_DB_DATA_DIR="/data/graph"
export GRAPH_DB_LOG_LEVEL="debug"
export GRAPH_DB_MAX_MEMORY="2GB"
export GRAPH_DB_CACHE_SIZE="512MB"
```

---

## 🚀 7. 性能基准

### 吞吐量目标
- **查询QPS**: 10,000+ queries/second
- **写入TPS**: 5,000+ writes/second
- **混合负载**: 3,000+ mixed ops/second

### 延迟目标
- **读取延迟**: P50 < 1ms, P99 < 10ms
- **写入延迟**: P50 < 5ms, P99 < 50ms
- **查询延迟**: P50 < 10ms, P99 < 100ms

### 资源使用
- **内存效率**: < 1KB/vertex + edges
- **存储效率**: < 100 bytes/edge
- **CPU使用**: < 80% on 8-core machine
- **磁盘I/O**: < 100MB/s sequential

---

## 🔒 8. 安全规范

### 认证机制

**JWT Token：**
```json
{
    "sub": "user123",
    "exp": 1640995200,
    "roles": ["read", "write"],
    "permissions": ["vertices:read", "edges:write"]
}
```

**API密钥：**
```http
Authorization: Bearer <jwt_token>
X-API-Key: <api_key>
```

### 授权控制

**RBAC模型：**
- **角色**: admin, user, readonly
- **权限**: vertices:read, vertices:write, edges:read, edges:write
- **资源**: graph, vertices, edges, queries

**访问控制：**
```rust
enum Permission {
    VertexRead,
    VertexWrite,
    EdgeRead,
    EdgeWrite,
    QueryExecute,
}

struct User {
    id: UserId,
    roles: HashSet<Role>,
    permissions: HashSet<Permission>,
}
```

---

## 📊 9. 监控指标

### 系统指标

**性能指标：**
- `graph.vertex_count` - 当前节点数量
- `graph.edge_count` - 当前边数量
- `graph.query_latency_ms` - 查询延迟分布
- `graph.write_latency_ms` - 写入延迟分布
- `graph.memory_usage_bytes` - 内存使用量

**业务指标：**
- `graph.queries_per_second` - 查询QPS
- `graph.writes_per_second` - 写入TPS
- `graph.cache_hit_rate` - 缓存命中率
- `graph.error_rate` - 错误率

**资源指标：**
- `graph.cpu_usage_percent` - CPU使用率
- `graph.disk_io_bytes_per_sec` - 磁盘I/O
- `graph.network_bytes_per_sec` - 网络吞吐
- `graph.open_file_descriptors` - 文件描述符数量

### 告警规则

**阈值设置：**
- **高延迟**: P99查询延迟 > 100ms
- **高错误率**: 错误率 > 1%
- **资源压力**: CPU > 90%, Memory > 80%
- **容量警告**: 磁盘使用 > 85%

---

## 🧪 10. 测试规范

### 单元测试

**覆盖率目标：**
- **代码覆盖率**: > 90%
- **分支覆盖率**: > 85%
- **函数覆盖率**: > 95%

**测试类别：**
- **功能测试**: 验证API行为
- **边界测试**: 测试极端输入
- **错误测试**: 验证异常处理
- **性能测试**: 基准关键操作

### 集成测试

**测试场景：**
- **完整事务**: 提交、回滚、并发
- **数据一致性**: 崩溃恢复、WAL回放
- **API集成**: HTTP/gRPC端到端
- **负载测试**: 高并发、大数据量

### 性能测试

**基准测试：**
```rust
// 使用criterion进行微基准测试
fn bench_vertex_insertion(c: &mut Criterion) {
    c.bench_function("vertex_insertion", |b| {
        b.iter(|| {
            // 插入节点操作
        })
    });
}
```

---

## 📚 11. 文档规范

### API文档
- **OpenAPI 3.0**: REST API规范
- **gRPC Protobuf**: 服务定义
- **代码示例**: 多语言客户端
- **交互式文档**: Swagger UI

### 开发者指南
- **快速开始**: 环境搭建和第一个查询
- **架构指南**: 系统设计和扩展
- **最佳实践**: 性能调优和常见陷阱
- **故障排除**: 常见问题和解决方案

---

这份技术规范为图数据库的开发、部署和维护提供了全面的指导，确保系统的高质量、高性能和高可靠性。