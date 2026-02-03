//! Distributed Computing Module
//!
//! This module provides distributed computing capabilities for the graph database,
//! including cluster management, distributed query processing, data partitioning,
//! and fault tolerance.
//!
//! ## Core Components:
//! - `cluster`: Cluster management and node discovery
//! - `consensus`: Distributed consensus mechanisms (Raft)
//! - `partitioning`: Data partitioning and distribution strategies
//! - `distributed_query`: Distributed query processing and execution
//! - `fault_tolerance`: Failure detection and recovery mechanisms
//! - `node_coordination`: Inter-node communication and coordination
//!
//! ## Integration Points:
//! - Extends existing query routing for distributed execution
//! - Integrates with materialized views for distributed caching
//! - Connects to streaming for distributed data processing

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::net::{SocketAddr, IpAddr};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::time::{interval, sleep};
use tokio::sync::{mpsc, broadcast};
use tracing::{info, warn, error, debug, trace};
use sha2::{Sha256, Digest};
use rand::{thread_rng, Rng};

use graph_core::{VertexId, Edge, PropertyValue, Error as CoreError};

type Properties = HashMap<String, PropertyValue>;

/// Distributed node information
#[derive(Debug, Clone)]
pub struct DistributedNode {
    /// Unique node identifier
    pub node_id: String,
    /// Network address
    pub address: SocketAddr,
    /// Node role in cluster
    pub role: NodeRole,
    /// Current load (0.0-1.0)
    pub load_factor: f64,
    /// Supported capabilities
    pub capabilities: NodeCapabilities,
    /// Last heartbeat timestamp
    pub last_heartbeat: Option<SystemTime>,
    /// Node metadata
    pub metadata: HashMap<String, String>,
}

/// Node roles in distributed cluster
#[derive(Debug, Clone, PartialEq)]
pub enum NodeRole {
    /// Coordinator node (manages cluster state)
    Coordinator,
    /// Worker node (processes queries)
    Worker,
    /// Storage node (manages data partitions)
    Storage,
    /// Cache node (manages distributed cache)
    Cache,
    /// Hybrid node (multiple roles)
    Hybrid,
}

/// Node capabilities
#[derive(Debug, Clone, PartialEq)]
pub struct NodeCapabilities {
    /// Can process queries
    pub can_process_queries: bool,
    /// Can store data partitions
    pub can_store_data: bool,
    /// Can serve cache
    pub can_serve_cache: bool,
    /// Maximum concurrent connections
    pub max_connections: u32,
    /// Supported protocols
    pub protocols: Vec<String>,
}

/// Cluster topology and routing information
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    /// Cluster ID
    pub cluster_id: String,
    /// All nodes in cluster
    pub nodes: HashMap<String, DistributedNode>,
    /// Network partitions
    pub partitions: Vec<PartitionInfo>,
    /// Routing table
    pub routing_table: HashMap<String, Vec<String>>,
    /// Replication factors
    pub replication_factors: HashMap<String, u8>,
}

/// Data partition information
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition ID
    pub partition_id: String,
    /// Nodes responsible for this partition
    pub responsible_nodes: Vec<String>,
    /// Partition range (for range-based partitioning)
    pub partition_range: Option<PartitionRange>,
    /// Current data size
    pub data_size_bytes: u64,
    /// Replication factor
    pub replication_factor: u8,
}

/// Partition range for data distribution
#[derive(Debug, Clone)]
pub struct PartitionRange {
    /// Start key/value
    pub start: String,
    /// End key/value
    pub end: String,
    /// Whether this is a hash partition
    pub is_hash_based: bool,
}

/// Distributed query execution context
#[derive(Debug, Clone)]
pub struct DistributedQueryContext {
    /// Query ID
    pub query_id: String,
    /// Source node ID
    pub source_node: String,
    /// Target nodes for execution
    pub target_nodes: Vec<String>,
    /// Execution strategy
    pub strategy: ExecutionStrategy,
    /// Query deadline
    pub deadline: Option<SystemTime>,
    /// Query parameters
    pub parameters: HashMap<String, PropertyValue>,
}

/// Query execution strategies
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionStrategy {
    /// Broadcast to all nodes
    Broadcast,
    /// Send to specific nodes only
    Selective(Vec<String>),
    /// Parallel execution with results aggregation
    Parallel,
    /// Sequential execution with fallback
    Sequential,
    /// Adaptive based on cluster state
    Adaptive,
}

/// Distributed cache coordination
#[derive(Debug, Clone)]
pub struct CacheCoordination {
    /// Cache nodes in cluster
    pub cache_nodes: HashMap<String, DistributedNode>,
    /// Cache coherence protocol
    pub coherence_protocol: CacheCoherenceProtocol,
    /// Cache invalidation strategy
    pub invalidation_strategy: InvalidationStrategy,
    /// Local cache size per node
    pub local_cache_sizes: HashMap<String, u64>,
}

/// Cache coherence protocols
#[derive(Debug, Clone, PartialEq)]
pub enum CacheCoherenceProtocol {
    /// Write-through with invalidation
    WriteThrough,
    /// Write-back with lazy consistency
    WriteBack,
    /// Write-around with direct access
    WriteAround,
    /// Directory-based coherence
    Directory,
}

/// Cache invalidation strategies
#[derive(Debug, Clone, PartialEq)]
pub enum InvalidationStrategy {
    /// Immediate invalidation
    Immediate,
    /// Delayed invalidation (batch)
    Delayed(Duration),
    /// Predictive invalidation (based on access patterns)
    Predictive,
    /// Hierarchical invalidation
    Hierarchical,
}

/// Distributed transaction context
#[derive(Debug, Clone)]
pub struct DistributedTransaction {
    /// Transaction ID
    pub transaction_id: String,
    /// Coordinator node
    pub coordinator: String,
    /// Participating nodes
    pub participants: Vec<String>,
    /// Transaction phase
    pub phase: TransactionPhase,
    /// Prepared operations
    pub prepared_operations: Vec<TransactionOperation>,
    /// Commit deadline
    pub commit_deadline: SystemTime,
}

/// Transaction phases
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionPhase {
    /// Preparing phase (locking resources)
    Preparing,
    /// Prepared phase (resources locked)
    Prepared,
    /// Committing phase (applying changes)
    Committing,
    /// Aborted phase (transaction cancelled)
    Aborted,
    /// Committed phase (transaction complete)
    Committed,
}

/// Transaction operation
#[derive(Debug, Clone)]
pub struct TransactionOperation {
    /// Operation ID
    pub operation_id: String,
    /// Operation type
    pub operation_type: OperationType,
    /// Target data
    pub target_data: TargetData,
    /// Operation parameters
    pub parameters: HashMap<String, PropertyValue>,
}

/// Operation types
#[derive(Debug, Clone, PartialEq)]
pub enum OperationType {
    /// Vertex insertion
    InsertVertex { vertex_id: VertexId, properties: Properties },
    /// Vertex update
    UpdateVertex { vertex_id: VertexId, old_properties: Properties, new_properties: Properties },
    /// Vertex deletion
    DeleteVertex { vertex_id: VertexId, properties: Properties },
    /// Edge insertion
    InsertEdge { edge: Edge, properties: Properties },
    /// Edge update
    UpdateEdge { edge: Edge, old_properties: Properties, new_properties: Properties },
    /// Edge deletion
    DeleteEdge { edge: Edge, properties: Properties },
}

/// Target data specification
#[derive(Debug, Clone)]
pub enum TargetData {
    /// Specific vertex
    Vertex(VertexId),
    /// Specific edge
    Edge(Edge),
    /// Pattern-based selection
    Pattern(String),
    /// Range-based selection
    Range { start: String, end: String },
    /// All data
    All,
}

/// Fault detection and recovery
#[derive(Debug, Clone)]
pub struct FaultDetector {
    /// Active fault detectors
    pub detectors: Vec<FaultType>,
    /// Fault history
    pub fault_history: Vec<FaultEvent>,
    /// Recovery strategies
    pub recovery_strategies: HashMap<FaultType, RecoveryStrategy>,
    /// Current cluster health
    pub cluster_health: ClusterHealth,
}

/// Fault types
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum FaultType {
    /// Node failure
    NodeFailure(String),
    /// Network partition
    NetworkPartition(Vec<String>),
    /// Data inconsistency
    DataInconsistency(String),
    /// Cache invalidation failure
    CacheInvalidation(String),
    /// Transaction timeout
    TransactionTimeout(String),
    /// Performance degradation
    PerformanceDegradation(String),
}

/// Fault events
#[derive(Debug, Clone)]
pub struct FaultEvent {
    /// Event ID
    pub event_id: String,
    /// Fault type
    pub fault_type: FaultType,
    /// Detection time
    pub detected_at: SystemTime,
    /// Affected nodes
    pub affected_nodes: Vec<String>,
    /// Recovery actions taken
    pub recovery_actions: Vec<String>,
    /// Resolution time
    pub resolved_at: Option<SystemTime>,
}

/// Recovery strategies
#[derive(Debug, Clone)]
pub struct RecoveryStrategy {
    /// Strategy name
    pub name: String,
    /// Automatic retry attempts
    pub retry_attempts: u8,
    /// Retry delay
    pub retry_delay: Duration,
    /// Fallback strategy
    pub fallback: Option<String>,
    /// Manual intervention required
    pub manual_intervention: bool,
}

/// Cluster health status
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterHealth {
    /// Healthy (all nodes operational)
    Healthy,
    /// Degraded (some nodes down)
    Degraded { failed_nodes: Vec<String> },
    /// Partitioned (network split)
    Partitioned { partition_groups: Vec<Vec<String>> },
    /// Recovering (in recovery process)
    Recovering,
}

/// Auto-scaling configuration
#[derive(Debug, Clone)]
pub struct AutoScalingConfig {
    /// Enable auto-scaling
    pub enabled: bool,
    /// Scale up threshold (load factor)
    pub scale_up_threshold: f64,
    /// Scale down threshold (load factor)
    pub scale_down_threshold: f64,
    /// Minimum nodes
    pub min_nodes: u8,
    /// Maximum nodes
    pub max_nodes: u16,
    /// Cooldown period between scalings
    pub scaling_cooldown: Duration,
    /// Evaluation window for load calculation
    pub evaluation_window: Duration,
}

impl Default for AutoScalingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            scale_up_threshold: 0.8,  // Scale up at 80% load
            scale_down_threshold: 0.3, // Scale down at 30% load
            min_nodes: 1,
            max_nodes: 100,
            scaling_cooldown: Duration::from_secs(300), // 5 minutes
            evaluation_window: Duration::from_secs(60), // 1 minute window
        }
    }
}

/// Metrics for distributed system
#[derive(Debug, Clone, Default)]
pub struct DistributedMetrics {
    /// Total queries processed
    pub queries_processed: u64,
    /// Average query latency
    pub avg_query_latency_ms: f64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
    /// Network message count
    pub network_messages: u64,
    /// Failed operations
    pub failed_operations: u64,
    /// Load balance efficiency
    pub load_balance_efficiency: f64,
    /// Data consistency score
    pub data_consistency_score: f64,
}

/// Error types for distributed operations
#[derive(Debug, thiserror::Error)]
pub enum DistributedError {
    #[error("Node not found: {node_id}")]
    NodeNotFound { node_id: String },
    
    #[error("Cluster not available: {cluster_id}")]
    ClusterUnavailable { cluster_id: String },
    
    #[error("Network partition detected: {affected_nodes:?}")]
    NetworkPartition { affected_nodes: Vec<String> },
    
    #[error("Transaction failed: {transaction_id}")]
    TransactionFailed { transaction_id: String },
    
    #[error("Cache coherence error: {details}")]
    CacheCoherenceError { details: String },
    
    #[error("Auto-scaling failed: {reason}")]
    AutoScalingFailed { reason: String },
    
    #[error("Insufficient nodes: {required}/{available}")]
    InsufficientNodes { required: u32, available: u32 },
    
    #[error("Connection failed: {node_id}: {reason}")]
    ConnectionFailed { node_id: String, reason: String },
    
    #[error("Serialization error: {details}")]
    SerializationError { details: String },
    
    #[error("Timeout expired: {operation}")]
    TimeoutExpired { operation: String },
}

/// Core cluster manager implementation
#[derive(Debug)]
pub struct ClusterManager {
    /// Current cluster topology
    topology: Arc<RwLock<ClusterTopology>>,
    /// Current node information
    current_node: DistributedNode,
    /// Heartbeat sender channel
    heartbeat_tx: mpsc::UnboundedSender<HeartbeatMessage>,
    /// Cluster events receiver
    events_rx: broadcast::Receiver<ClusterEvent>,
    /// Metrics collector
    metrics: Arc<RwLock<DistributedMetrics>>,
    /// Configuration
    config: ClusterConfig,
}

/// Cluster configuration
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Node timeout threshold
    pub node_timeout: Duration,
    /// Replication factor for data
    pub default_replication_factor: u8,
    /// Maximum retries for operations
    pub max_retries: u32,
    /// Enable auto-scaling
    pub auto_scaling: AutoScalingConfig,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            node_timeout: Duration::from_secs(15),
            default_replication_factor: 3,
            max_retries: 3,
            auto_scaling: AutoScalingConfig::default(),
        }
    }
}

/// Heartbeat message sent between nodes
#[derive(Debug, Clone)]
pub struct HeartbeatMessage {
    /// Sending node ID
    pub node_id: String,
    /// Current timestamp
    pub timestamp: SystemTime,
    /// Current load factor
    pub load_factor: f64,
    /// Available memory (bytes)
    pub available_memory: u64,
    /// Active connections
    pub active_connections: u32,
    /// Health status
    pub health: NodeHealth,
}

/// Node health status
#[derive(Debug, Clone, PartialEq)]
pub enum NodeHealth {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

/// Cluster events
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    NodeJoined { node_id: String, node: DistributedNode },
    NodeLeft { node_id: String, reason: String },
    NodeFailed { node_id: String, last_seen: SystemTime },
    PartitionDetected { affected_nodes: Vec<String> },
    PartitionHealed { recovered_nodes: Vec<String> },
    LoadImbalance { overloaded_nodes: Vec<String>, underloaded_nodes: Vec<String> },
}

impl ClusterManager {
    /// Create a new cluster manager
    pub fn new(
        current_node: DistributedNode,
        config: ClusterConfig,
    ) -> Result<(Self, broadcast::Receiver<ClusterEvent>), DistributedError> {
        let (heartbeat_tx, heartbeat_rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = broadcast::channel(1000);

        let cluster_id = generate_cluster_id(&current_node.node_id);
        
        let topology = Arc::new(RwLock::new(ClusterTopology {
            cluster_id,
            nodes: HashMap::new(),
            partitions: Vec::new(),
            routing_table: HashMap::new(),
            replication_factors: HashMap::new(),
        }));

        let metrics = Arc::new(RwLock::new(DistributedMetrics::default()));

        let manager = Self {
            topology,
            current_node,
            heartbeat_tx,
            events_rx,
            metrics,
            config,
        };

        // Start background tasks
        manager.start_heartbeat_sender()?;
        manager.start_heartbeat_monitor(heartbeat_rx)?;
        manager.start_health_checker()?;

        Ok((manager, events_rx))
    }

    /// Join a cluster by connecting to known nodes
    pub async fn join_cluster(&self, known_nodes: Vec<SocketAddr>) -> Result<(), DistributedError> {
        info!("Joining cluster with {} known nodes", known_nodes.len());

        for addr in known_nodes {
            match self.connect_to_node(addr).await {
                Ok(node) => {
                    info!("Successfully connected to node: {}", node.node_id);
                    self.add_node_to_topology(node).await?;
                }
                Err(e) => {
                    warn!("Failed to connect to node at {}: {}", addr, e);
                }
            }
        }

        self.initialize_partitions().await?;
        Ok(())
    }

    /// Add a node to the cluster topology
    pub async fn add_node_to_topology(&self, node: DistributedNode) -> Result<(), DistributedError> {
        let mut topology = self.topology.write().unwrap();
        
        if topology.nodes.contains_key(&node.node_id) {
            return Err(DistributedError::NodeNotFound { 
                node_id: format!("Node {} already exists", node.node_id) 
            });
        }

        topology.nodes.insert(node.node_id.clone(), node.clone());
        
        // Update routing table
        self.update_routing_table(&mut topology);
        
        drop(topology);

        // Broadcast node joined event
        let _ = self.broadcast_event(ClusterEvent::NodeJoined { 
            node_id: node.node_id.clone(), 
            node 
        });

        info!("Added node {} to cluster", node.node_id);
        Ok(())
    }

    /// Get current cluster topology
    pub fn get_topology(&self) -> ClusterTopology {
        self.topology.read().unwrap().clone()
    }

    /// Get current cluster metrics
    pub fn get_metrics(&self) -> DistributedMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Select nodes for query execution based on strategy
    pub fn select_nodes_for_query(
        &self, 
        context: &DistributedQueryContext,
    ) -> Result<Vec<String>, DistributedError> {
        let topology = self.topology.read().unwrap();
        
        match &context.strategy {
            ExecutionStrategy::Broadcast => {
                Ok(topology.nodes.keys().cloned().collect())
            }
            ExecutionStrategy::Selective(node_ids) => {
                // Verify all requested nodes are available
                for node_id in node_ids {
                    if !topology.nodes.contains_key(node_id) {
                        return Err(DistributedError::NodeNotFound { 
                            node_id: node_id.clone() 
                        });
                    }
                }
                Ok(node_ids.clone())
            }
            ExecutionStrategy::Parallel => {
                // Select nodes with lowest load factor
                let mut available_nodes: Vec<_> = topology.nodes.iter()
                    .filter(|(_, node)| node.load_factor < 0.8)
                    .collect();
                    
                available_nodes.sort_by(|a, b| a.1.load_factor.partial_cmp(&b.1.load_factor).unwrap());
                
                let selected_nodes: Vec<String> = available_nodes
                    .into_iter()
                    .take(3) // Use top 3 least loaded nodes
                    .map(|(id, _)| id.clone())
                    .collect();
                    
                Ok(selected_nodes)
            }
            ExecutionStrategy::Adaptive => {
                // Intelligent selection based on query patterns and node capabilities
                self.adaptive_node_selection(&context, &topology)
            }
            ExecutionStrategy::Sequential => {
                // Select single best node for sequential execution
                let best_node = topology.nodes.iter()
                    .filter(|(_, node)| node.capabilities.can_process_queries)
                    .min_by(|a, b| a.1.load_factor.partial_cmp(&b.1.load_factor).unwrap());
                    
                match best_node {
                    Some((node_id, _)) => Ok(vec![node_id.clone()]),
                    None => Err(DistributedError::InsufficientNodes { 
                        required: 1, 
                        available: 0 
                    }),
                }
            }
        }
    }

    // Private helper methods
    
    fn start_heartbeat_sender(&self) -> Result<(), DistributedError> {
        let tx = self.heartbeat_tx.clone();
        let node_id = self.current_node.node_id.clone();
        let interval = self.config.heartbeat_interval;

        tokio::spawn(async move {
            let mut interval_timer = interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                let heartbeat = HeartbeatMessage {
                    node_id: node_id.clone(),
                    timestamp: SystemTime::now(),
                    load_factor: 0.5, // TODO: Calculate actual load
                    available_memory: 1024 * 1024 * 1024, // TODO: Get actual memory
                    active_connections: 10, // TODO: Get actual connections
                    health: NodeHealth::Healthy,
                };

                if let Err(_) = tx.send(heartbeat) {
                    error!("Failed to send heartbeat - channel closed");
                    break;
                }
            }
        });

        Ok(())
    }

    fn start_heartbeat_monitor(&self, mut rx: mpsc::UnboundedReceiver<HeartbeatMessage>) -> Result<(), DistributedError> {
        let topology = self.topology.clone();
        let config = self.config.clone();
        
        tokio::spawn(async move {
            while let Some(heartbeat) = rx.recv().await {
                let mut topo = topology.write().unwrap();
                
                if let Some(node) = topo.nodes.get_mut(&heartbeat.node_id) {
                    node.last_heartbeat = Some(heartbeat.timestamp);
                    node.load_factor = heartbeat.load_factor;
                    
                    trace!("Updated heartbeat for node: {}", heartbeat.node_id);
                } else {
                    debug!("Received heartbeat from unknown node: {}", heartbeat.node_id);
                }
            }
        });

        Ok(())
    }

    fn start_health_checker(&self) -> Result<(), DistributedError> {
        let topology = self.topology.clone();
        let timeout = self.config.node_timeout;
        
        tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(10));
            
            loop {
                interval_timer.tick().await;
                
                let now = SystemTime::now();
                let mut failed_nodes = Vec::new();
                
                {
                    let topo = topology.read().unwrap();
                    for (node_id, node) in topo.nodes.iter() {
                        if let Some(last_heartbeat) = node.last_heartbeat {
                            if now.duration_since(last_heartbeat).unwrap_or(Duration::MAX) > timeout {
                                failed_nodes.push(node_id.clone());
                            }
                        }
                    }
                }
                
                for node_id in failed_nodes {
                    warn!("Node {} failed health check", node_id);
                    // TODO: Handle node failure
                }
            }
        });

        Ok(())
    }

    fn update_routing_table(&self, topology: &mut ClusterTopology) {
        topology.routing_table.clear();
        
        // Simple hash-based routing for now
        for (node_id, node) in topology.nodes.iter() {
            if node.capabilities.can_store_data {
                topology.routing_table.insert("data".to_string(), vec![node_id.clone()]);
            }
            if node.capabilities.can_process_queries {
                topology.routing_table.insert("query".to_string(), vec![node_id.clone()]);
            }
            if node.capabilities.can_serve_cache {
                topology.routing_table.insert("cache".to_string(), vec![node_id.clone()]);
            }
        }
    }

    fn initialize_partitions(&self) -> Result<(), DistributedError> {
        let mut topology = self.topology.write().unwrap();
        
        // Create default partitions based on consistent hashing
        let partition_count = topology.nodes.len().max(1);
        topology.partitions.clear();
        
        for i in 0..partition_count {
            let partition = PartitionInfo {
                partition_id: format!("partition_{}", i),
                responsible_nodes: vec![], // Will be filled by routing logic
                partition_range: Some(PartitionRange {
                    start: format!("hash_{}", i),
                    end: format!("hash_{}", i + 1),
                    is_hash_based: true,
                }),
                data_size_bytes: 0,
                replication_factor: self.config.default_replication_factor,
            };
            topology.partitions.push(partition);
        }
        
        info!("Initialized {} partitions", partition_count);
        Ok(())
    }

    async fn connect_to_node(&self, addr: SocketAddr) -> Result<DistributedNode, DistributedError> {
        // TODO: Implement actual node discovery protocol
        // For now, create a mock node
        let node = DistributedNode {
            node_id: format!("node_{}", thread_rng().gen::<u32>()),
            address: addr,
            role: NodeRole::Worker,
            load_factor: 0.3,
            capabilities: NodeCapabilities {
                can_process_queries: true,
                can_store_data: true,
                can_serve_cache: false,
                max_connections: 1000,
                protocols: vec!["grpc".to_string()],
            },
            last_heartbeat: Some(SystemTime::now()),
            metadata: HashMap::new(),
        };
        
        Ok(node)
    }

    fn adaptive_node_selection(
        &self,
        context: &DistributedQueryContext,
        topology: &ClusterTopology,
    ) -> Result<Vec<String>, DistributedError> {
        // Intelligent selection based on query parameters and node capabilities
        let mut candidates: Vec<_> = topology.nodes.iter()
            .filter(|(_, node)| node.capabilities.can_process_queries)
            .collect();
            
        // Score nodes based on multiple factors
        let mut scored_nodes = Vec::new();
        for (node_id, node) in candidates {
            let mut score = 0.0;
            
            // Load factor (lower is better)
            score += (1.0 - node.load_factor) * 0.4;
            
            // Role preference
            match node.role {
                NodeRole::Worker | NodeRole::Hybrid => score += 0.3,
                NodeRole::Coordinator => score += 0.2,
                _ => {}
            }
            
            // Connection capacity
            score += (node.capabilities.max_connections as f64 / 1000.0) * 0.2;
            
            // Protocol compatibility
            if node.protocols.contains(&"grpc".to_string()) {
                score += 0.1;
            }
            
            scored_nodes.push((node_id.clone(), score));
        }
        
        // Sort by score and select top nodes
        scored_nodes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        let selected_nodes: Vec<String> = scored_nodes
            .into_iter()
            .take(3) // Top 3 nodes
            .map(|(id, _)| id)
            .collect();
            
        Ok(selected_nodes)
    }

    fn broadcast_event(&self, event: ClusterEvent) -> Result<(), DistributedError> {
        // TODO: Implement event broadcasting
        debug!("Broadcasting cluster event: {:?}", event);
        Ok(())
    }
}

/// Generate unique cluster ID
fn generate_cluster_id(seed: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(seed.as_bytes());
    hasher.update(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().to_string().as_bytes());
    format!("cluster_{:x}", hasher.finalize())[..16].to_string()
}

/// Distributed query executor
#[derive(Debug)]
pub struct DistributedQueryExecutor {
    /// Cluster manager reference
    cluster_manager: Arc<ClusterManager>,
    /// Query execution metrics
    query_metrics: Arc<RwLock<QueryExecutionMetrics>>,
    /// Active queries tracker
    active_queries: Arc<RwLock<HashMap<String, DistributedQueryContext>>>,
    /// Configuration
    config: QueryExecutorConfig,
}

/// Query execution metrics
#[derive(Debug, Clone, Default)]
pub struct QueryExecutionMetrics {
    /// Total queries executed
    pub total_queries: u64,
    /// Successful queries
    pub successful_queries: u64,
    /// Failed queries
    pub failed_queries: u64,
    /// Average execution time
    pub avg_execution_time_ms: f64,
    /// Parallel query count
    pub parallel_queries: u64,
    /// Network hops per query (average)
    pub avg_network_hops: f64,
}

/// Query executor configuration
#[derive(Debug, Clone)]
pub struct QueryExecutorConfig {
    /// Query timeout
    pub query_timeout: Duration,
    /// Maximum parallel nodes per query
    pub max_parallel_nodes: u8,
    /// Enable query result caching
    pub enable_result_cache: bool,
    /// Result cache TTL
    pub result_cache_ttl: Duration,
    /// Enable query batching
    pub enable_batching: bool,
    /// Maximum batch size
    pub max_batch_size: u32,
}

impl Default for QueryExecutorConfig {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(30),
            max_parallel_nodes: 5,
            enable_result_cache: true,
            result_cache_ttl: Duration::from_secs(300),
            enable_batching: true,
            max_batch_size: 100,
        }
    }
}

/// Query execution result
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Query ID
    pub query_id: String,
    /// Execution status
    pub status: QueryStatus,
    /// Result data
    pub data: Option<QueryData>,
    /// Execution time
    pub execution_time_ms: u64,
    /// Nodes involved
    pub involved_nodes: Vec<String>,
    /// Error if failed
    pub error: Option<String>,
    /// Performance metrics
    pub metrics: QueryPerformanceMetrics,
}

/// Query execution status
#[derive(Debug, Clone, PartialEq)]
pub enum QueryStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Timeout,
    Cancelled,
}

/// Query data representation
#[derive(Debug, Clone)]
pub enum QueryData {
    /// Vertex data
    Vertices(Vec<VertexData>),
    /// Edge data
    Edges(Vec<EdgeData>),
    /// Aggregate result
    Aggregate(AggregateResult),
    /// Analytics result
    Analytics(AnalyticsResult),
    /// Custom data
    Custom { data_type: String, payload: Vec<u8> },
}

/// Vertex data
#[derive(Debug, Clone)]
pub struct VertexData {
    pub vertex_id: VertexId,
    pub properties: Properties,
    pub partition_id: Option<String>,
}

/// Edge data
#[derive(Debug, Clone)]
pub struct EdgeData {
    pub edge: Edge,
    pub properties: Properties,
    pub partition_id: Option<String>,
}

/// Aggregate result
#[derive(Debug, Clone)]
pub struct AggregateResult {
    pub aggregate_type: String,
    pub result: PropertyValue,
    pub count: u64,
    pub groups: HashMap<String, PropertyValue>,
}

/// Analytics result
#[derive(Debug, Clone)]
pub struct AnalyticsResult {
    pub algorithm: String,
    pub result_data: Vec<u8>,
    pub execution_stats: AnalyticsStats,
}

/// Analytics statistics
#[derive(Debug, Clone)]
pub struct AnalyticsStats {
    pub vertices_processed: u64,
    pub edges_processed: u64,
    pub iterations: u32,
    pub convergence_time_ms: u64,
}

/// Query performance metrics
#[derive(Debug, Clone)]
pub struct QueryPerformanceMetrics {
    /// Network round trips
    pub network_round_trips: u32,
    /// Data transferred (bytes)
    pub data_transferred: u64,
    /// Cache hits
    pub cache_hits: u32,
    /// Cache misses
    pub cache_misses: u32,
    /// Parallelization factor
    pub parallelization_factor: u8,
}

impl DistributedQueryExecutor {
    /// Create new distributed query executor
    pub fn new(cluster_manager: Arc<ClusterManager>) -> Self {
        Self::with_config(cluster_manager, QueryExecutorConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(cluster_manager: Arc<ClusterManager>, config: QueryExecutorConfig) -> Self {
        Self {
            cluster_manager,
            query_metrics: Arc::new(RwLock::new(QueryExecutionMetrics::default())),
            active_queries: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Execute a distributed query
    pub async fn execute_query(
        &self,
        query_context: DistributedQueryContext,
    ) -> Result<QueryResult, DistributedError> {
        let start_time = SystemTime::now();
        let query_id = query_context.query_id.clone();

        info!("Executing distributed query: {}", query_id);

        // Track active query
        {
            let mut active = self.active_queries.write().unwrap();
            active.insert(query_id.clone(), query_context.clone());
        }

        let result = match self.execute_query_internal(&query_context).await {
            Ok(mut result) => {
                let execution_time = start_time.duration_since(SystemTime::now()).unwrap_or_default();
                result.execution_time_ms = execution_time.as_millis() as u64;
                result.status = QueryStatus::Completed;
                
                // Update metrics
                self.update_success_metrics(&result);
                Ok(result)
            }
            Err(e) => {
                let execution_time = start_time.duration_since(SystemTime::now()).unwrap_or_default();
                
                let error_result = QueryResult {
                    query_id: query_id.clone(),
                    status: QueryStatus::Failed,
                    data: None,
                    execution_time_ms: execution_time.as_millis() as u64,
                    involved_nodes: Vec::new(),
                    error: Some(e.to_string()),
                    metrics: QueryPerformanceMetrics::default(),
                };

                self.update_failure_metrics(&error_result);
                Ok(error_result)
            }
        };

        // Remove from active queries
        {
            let mut active = self.active_queries.write().unwrap();
            active.remove(&query_id);
        }

        result
    }

    /// Cancel an active query
    pub async fn cancel_query(&self, query_id: &str) -> Result<(), DistributedError> {
        let mut active = self.active_queries.write().unwrap();
        
        if let Some(query_context) = active.remove(query_id) {
            info!("Cancelling query: {}", query_id);
            
            // Notify all involved nodes to cancel
            for node_id in query_context.target_nodes {
                // TODO: Send cancellation message to node
                debug!("Sending cancellation to node: {}", node_id);
            }
            
            Ok(())
        } else {
            Err(DistributedError::TransactionFailed { 
                transaction_id: query_id.to_string() 
            })
        }
    }

    /// Get query execution metrics
    pub fn get_query_metrics(&self) -> QueryExecutionMetrics {
        self.query_metrics.read().unwrap().clone()
    }

    /// Get active queries
    pub fn get_active_queries(&self) -> Vec<DistributedQueryContext> {
        self.active_queries.read().unwrap().values().cloned().collect()
    }

    // Private methods

    async fn execute_query_internal(
        &self,
        query_context: &DistributedQueryContext,
    ) -> Result<QueryResult, DistributedError> {
        // Select target nodes for execution
        let target_nodes = self.cluster_manager.select_nodes_for_query(query_context)?;
        
        if target_nodes.is_empty() {
            return Err(DistributedError::InsufficientNodes { 
                required: 1, 
                available: 0 
            });
        }

        info!("Selected {} nodes for query execution", target_nodes.len());

        // Execute based on strategy
        match &query_context.strategy {
            ExecutionStrategy::Broadcast => {
                self.execute_broadcast_query(query_context, &target_nodes).await
            }
            ExecutionStrategy::Parallel => {
                self.execute_parallel_query(query_context, &target_nodes).await
            }
            ExecutionStrategy::Sequential => {
                self.execute_sequential_query(query_context, &target_nodes).await
            }
            ExecutionStrategy::Selective(_) => {
                self.execute_selective_query(query_context, &target_nodes).await
            }
            ExecutionStrategy::Adaptive => {
                self.execute_adaptive_query(query_context, &target_nodes).await
            }
        }
    }

    async fn execute_broadcast_query(
        &self,
        query_context: &DistributedQueryContext,
        target_nodes: &[String],
    ) -> Result<QueryResult, DistributedError> {
        info!("Executing broadcast query on {} nodes", target_nodes.len());

        let mut futures = Vec::new();
        for node_id in target_nodes {
            let future = self.send_query_to_node(query_context, node_id);
            futures.push(future);
        }

        // Wait for all responses
        let results = futures::future::join_all(futures).await;
        
        // Aggregate results
        let mut all_data = Vec::new();
        let mut involved_nodes = Vec::new();
        let mut network_round_trips = 0;
        let mut data_transferred = 0;

        for (node_id, result) in results.into_iter().zip(target_nodes.iter()) {
            match result {
                Ok(node_result) => {
                    all_data.push(node_result);
                    involved_nodes.push(node_id.clone());
                    network_round_trips += 1;
                    // TODO: Calculate actual data transferred
                }
                Err(e) => {
                    warn!("Query failed on node {}: {}", node_id, e);
                    // Continue with other nodes for broadcast
                }
            }
        }

        // Combine results from all nodes
        let combined_data = self.combine_query_results(all_data).await?;

        Ok(QueryResult {
            query_id: query_context.query_id.clone(),
            status: QueryStatus::Completed,
            data: Some(combined_data),
            execution_time_ms: 0, // Will be set by caller
            involved_nodes,
            error: None,
            metrics: QueryPerformanceMetrics {
                network_round_trips,
                data_transferred,
                cache_hits: 0,
                cache_misses: 0,
                parallelization_factor: target_nodes.len() as u8,
            },
        })
    }

    async fn execute_parallel_query(
        &self,
        query_context: &DistributedQueryContext,
        target_nodes: &[String],
    ) -> Result<QueryResult, DistributedError> {
        info!("Executing parallel query on {} nodes", target_nodes.len());

        // For parallel execution, partition the query across nodes
        let parallel_nodes = if target_nodes.len() > self.config.max_parallel_nodes as usize {
            &target_nodes[..self.config.max_parallel_nodes as usize]
        } else {
            target_nodes
        };

        let mut futures = Vec::new();
        for (i, node_id) in parallel_nodes.iter().enumerate() {
            let partitioned_context = self.partition_query_context(query_context, i, parallel_nodes.len());
            let future = self.send_query_to_node(&partitioned_context, node_id);
            futures.push(future);
        }

        // Wait for all parallel results
        let results = futures::future::join_all(futures).await;
        
        // Merge parallel results
        self.merge_parallel_results(query_context, results, parallel_nodes).await
    }

    async fn execute_sequential_query(
        &self,
        query_context: &DistributedQueryContext,
        target_nodes: &[String],
    ) -> Result<QueryResult, DistributedError> {
        info!("Executing sequential query on {} nodes", target_nodes.len());

        let mut cumulative_result = None;
        let mut involved_nodes = Vec::new();
        let mut total_round_trips = 0;

        for node_id in target_nodes {
            let result = self.send_query_to_node(query_context, node_id).await?;
            
            cumulative_result = Some(result);
            involved_nodes.push(node_id.clone());
            total_round_trips += 1;
            
            // For sequential queries, we might stop early if we get the desired result
            // TODO: Implement early termination logic
        }

        match cumulative_result {
            Some(final_result) => Ok(QueryResult {
                query_id: query_context.query_id.clone(),
                status: QueryStatus::Completed,
                data: Some(final_result),
                execution_time_ms: 0,
                involved_nodes,
                error: None,
                metrics: QueryPerformanceMetrics {
                    network_round_trips: total_round_trips,
                    data_transferred: 0,
                    cache_hits: 0,
                    cache_misses: 0,
                    parallelization_factor: 1,
                },
            }),
            None => Err(DistributedError::TransactionFailed { 
                transaction_id: query_context.query_id.clone() 
            }),
        }
    }

    async fn execute_selective_query(
        &self,
        query_context: &DistributedQueryContext,
        target_nodes: &[String],
    ) -> Result<QueryResult, DistributedError> {
        info!("Executing selective query on {} nodes", target_nodes.len());

        // Similar to sequential but only on specific nodes
        self.execute_sequential_query(query_context, target_nodes).await
    }

    async fn execute_adaptive_query(
        &self,
        query_context: &DistributedQueryContext,
        target_nodes: &[String],
    ) -> Result<QueryResult, DistributedError> {
        info!("Executing adaptive query on {} nodes", target_nodes.len());

        // Choose strategy based on query characteristics
        let strategy = self.determine_optimal_strategy(query_context, target_nodes);
        
        match strategy {
            ExecutionStrategy::Parallel => {
                self.execute_parallel_query(query_context, target_nodes).await
            }
            ExecutionStrategy::Broadcast => {
                self.execute_broadcast_query(query_context, target_nodes).await
            }
            _ => {
                self.execute_sequential_query(query_context, target_nodes).await
            }
        }
    }

    async fn send_query_to_node(
        &self,
        query_context: &DistributedQueryContext,
        node_id: &str,
    ) -> Result<QueryData, DistributedError> {
        // TODO: Implement actual RPC to node
        debug!("Sending query to node: {}", node_id);
        
        // Mock result for now
        Ok(QueryData::Custom {
            data_type: "mock_result".to_string(),
            payload: vec![1, 2, 3, 4],
        })
    }

    fn partition_query_context(
        &self,
        original: &DistributedQueryContext,
        partition_index: usize,
        total_partitions: usize,
    ) -> DistributedQueryContext {
        // Create a partitioned version of the query context
        let mut partitioned = original.clone();
        partitioned.query_id = format!("{}_part_{}", original.query_id, partition_index);
        
        // TODO: Add partition-specific parameters
        partitioned
    }

    async fn combine_query_results(&self, results: Vec<QueryData>) -> Result<QueryData, DistributedError> {
        // TODO: Implement result combination logic
        if results.is_empty() {
            return Ok(QueryData::Custom {
                data_type: "empty".to_string(),
                payload: Vec::new(),
            });
        }

        // For now, return the first result
        Ok(results.into_iter().next().unwrap())
    }

    async fn merge_parallel_results(
        &self,
        query_context: &DistributedQueryContext,
        results: Vec<Result<QueryData, DistributedError>>,
        target_nodes: &[String],
    ) -> Result<QueryResult, DistributedError> {
        let mut successful_results = Vec::new();
        let mut involved_nodes = Vec::new();
        let mut failed_nodes = 0;

        for (result, node_id) in results.into_iter().zip(target_nodes.iter()) {
            match result {
                Ok(data) => {
                    successful_results.push(data);
                    involved_nodes.push(node_id.clone());
                }
                Err(e) => {
                    warn!("Parallel query failed on node {}: {}", node_id, e);
                    failed_nodes += 1;
                }
            }
        }

        if successful_results.is_empty() {
            return Err(DistributedError::TransactionFailed { 
                transaction_id: query_context.query_id.clone() 
            });
        }

        let combined_data = self.combine_query_results(successful_results).await?;

        Ok(QueryResult {
            query_id: query_context.query_id.clone(),
            status: QueryStatus::Completed,
            data: Some(combined_data),
            execution_time_ms: 0,
            involved_nodes,
            error: None,
            metrics: QueryPerformanceMetrics {
                network_round_trips: target_nodes.len() as u32,
                data_transferred: 0,
                cache_hits: 0,
                cache_misses: 0,
                parallelization_factor: target_nodes.len() as u8,
            },
        })
    }

    fn determine_optimal_strategy(
        &self,
        query_context: &DistributedQueryContext,
        target_nodes: &[String],
    ) -> ExecutionStrategy {
        // Simple heuristic: use parallel for multiple nodes, broadcast for many
        if target_nodes.len() > 5 {
            ExecutionStrategy::Broadcast
        } else if target_nodes.len() > 1 {
            ExecutionStrategy::Parallel
        } else {
            ExecutionStrategy::Sequential
        }
    }

    fn update_success_metrics(&self, result: &QueryResult) {
        let mut metrics = self.query_metrics.write().unwrap();
        metrics.total_queries += 1;
        metrics.successful_queries += 1;
        
        // Update average execution time
        let new_time = result.execution_time_ms as f64;
        let total_time = metrics.avg_execution_time_ms * (metrics.successful_queries - 1) as f64;
        metrics.avg_execution_time_ms = (total_time + new_time) / metrics.successful_queries as f64;
        
        if result.metrics.parallelization_factor > 1 {
            metrics.parallel_queries += 1;
        }
    }

    fn update_failure_metrics(&self, _result: &QueryResult) {
        let mut metrics = self.query_metrics.write().unwrap();
        metrics.total_queries += 1;
        metrics.failed_queries += 1;
    }
}

/// Distributed cache coherence manager
#[derive(Debug)]
pub struct DistributedCacheManager {
    /// Cluster manager reference
    cluster_manager: Arc<ClusterManager>,
    /// Cache configuration
    config: CacheConfig,
    /// Local cache instance
    local_cache: Arc<RwLock<LocalCache>>,
    /// Cache coherence protocol
    coherence_protocol: Box<dyn CacheCoherenceProtocol>,
    /// Invalidation queue
    invalidation_tx: mpsc::UnboundedSender<InvalidationMessage>,
    /// Cache metrics
    metrics: Arc<RwLock<CacheMetrics>>,
}

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum cache size per node (bytes)
    pub max_cache_size: u64,
    /// Cache entry TTL
    pub entry_ttl: Duration,
    /// Cache coherence protocol
    pub coherence_protocol: CacheCoherenceProtocolType,
    /// Invalidation strategy
    pub invalidation_strategy: InvalidationStrategy,
    /// Enable hierarchical caching
    pub enable_hierarchical: bool,
    /// L1 cache size (hot data)
    pub l1_cache_size: u64,
    /// L2 cache size (warm data)  
    pub l2_cache_size: u64,
    /// L3 cache size (cold data)
    pub l3_cache_size: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_cache_size: 1024 * 1024 * 1024, // 1GB
            entry_ttl: Duration::from_secs(300), // 5 minutes
            coherence_protocol: CacheCoherenceProtocolType::Directory,
            invalidation_strategy: InvalidationStrategy::Immediate,
            enable_hierarchical: true,
            l1_cache_size: 64 * 1024 * 1024,    // 64MB
            l2_cache_size: 256 * 1024 * 1024,   // 256MB
            l3_cache_size: 704 * 1024 * 1024,   // 704MB
        }
    }
}

/// Cache coherence protocol types
#[derive(Debug, Clone)]
pub enum CacheCoherenceProtocolType {
    /// Write-through with invalidation
    WriteThrough,
    /// Write-back with lazy consistency
    WriteBack,
    /// Directory-based coherence
    Directory,
    /// Token-based coherence
    TokenBased,
}

/// Local cache implementation
#[derive(Debug, Default)]
pub struct LocalCache {
    /// L1 cache (hot data)
    l1_cache: HashMap<String, CacheEntry>,
    /// L2 cache (warm data)
    l2_cache: HashMap<String, CacheEntry>,
    /// L3 cache (cold data)
    l3_cache: HashMap<String, CacheEntry>,
    /// Current usage per level
    l1_usage: u64,
    l2_usage: u64,
    l3_usage: u64,
    /// Access patterns for promotion/demotion
    access_patterns: HashMap<String, AccessPattern>,
}

/// Cache entry
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Cache key
    pub key: String,
    /// Cached value
    pub value: CacheValue,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Last access timestamp
    pub last_accessed: SystemTime,
    /// Access count
    pub access_count: u64,
    /// Entry size (bytes)
    pub size: u64,
    /// Cache level
    pub level: CacheLevel,
    /// Validity status
    pub valid: bool,
}

/// Cache value types
#[derive(Debug, Clone)]
pub enum CacheValue {
    /// Vertex data
    Vertex(VertexData),
    /// Edge data
    Edge(EdgeData),
    /// Query result
    QueryResult(QueryResult),
    /// Aggregate data
    Aggregate(AggregateResult),
    /// Raw bytes
    Bytes(Vec<u8>),
}

/// Cache levels
#[derive(Debug, Clone, PartialEq)]
pub enum CacheLevel {
    L1, // Hot cache
    L2, // Warm cache
    L3, // Cold cache
}

/// Access pattern for cache entries
#[derive(Debug, Clone, Default)]
pub struct AccessPattern {
    /// Total access count
    pub total_accesses: u64,
    /// Recent access count (last hour)
    pub recent_accesses: u64,
    /// Last access time
    pub last_access: Option<SystemTime>,
    /// Access frequency (accesses per minute)
    pub access_frequency: f64,
}

/// Invalidation message
#[derive(Debug, Clone)]
pub struct InvalidationMessage {
    /// Invalidation ID
    pub invalidation_id: String,
    /// Cache key to invalidate
    pub cache_key: String,
    /// Invalidating node
    pub source_node: String,
    /// Invalidation reason
    pub reason: InvalidationReason,
    /// Timestamp
    pub timestamp: SystemTime,
    /// Target nodes (empty = all)
    pub target_nodes: Vec<String>,
}

/// Invalidation reasons
#[derive(Debug, Clone, PartialEq)]
pub enum InvalidationReason {
    DataUpdated,
    DataDeleted,
    TTLExpired,
    CacheEviction,
    ManualInvalidation,
    ConsistencyRequirement,
}

/// Cache coherence protocol trait
#[async_trait::async_trait]
pub trait CacheCoherenceProtocol: Send + Sync + std::fmt::Debug {
    /// Handle cache invalidation
    async fn handle_invalidation(&self, message: InvalidationMessage) -> Result<(), DistributedError>;
    
    /// Handle cache update
    async fn handle_update(&self, key: &str, value: &CacheValue) -> Result<(), DistributedError>;
    
    /// Check if entry is valid
    async fn is_valid(&self, key: &str) -> Result<bool, DistributedError>;
    
    /// Get coherence state for key
    async fn get_coherence_state(&self, key: &str) -> Result<CoherenceState, DistributedError>;
}

/// Coherence state for cache entries
#[derive(Debug, Clone)]
pub struct CoherenceState {
    /// Key
    pub key: String,
    /// Owning node
    pub owner_node: String,
    /// Valid nodes
    pub valid_nodes: Vec<String>,
    /// Invalidated nodes
    pub invalidated_nodes: Vec<String>,
    /// Last modification timestamp
    pub last_modified: SystemTime,
    /// Version number
    pub version: u64,
}

/// Cache metrics
#[derive(Debug, Clone, Default)]
pub struct CacheMetrics {
    /// Total cache hits
    pub total_hits: u64,
    /// Total cache misses
    pub total_misses: u64,
    /// Hit ratio (percentage)
    pub hit_ratio: f64,
    /// Invalidation operations
    pub invalidations: u64,
    /// Promotion operations (L2->L1, L3->L2)
    pub promotions: u64,
    /// Demotion operations (L1->L2, L2->L3)
    pub demotions: u64,
    /// Evictions due to space
    pub evictions: u64,
    /// Network coherence messages
    pub coherence_messages: u64,
}

/// Write-through coherence protocol
#[derive(Debug)]
pub struct WriteThroughProtocol {
    cluster_manager: Arc<ClusterManager>,
    local_cache: Arc<RwLock<LocalCache>>,
}

/// Directory-based coherence protocol
#[derive(Debug)]
pub struct DirectoryProtocol {
    cluster_manager: Arc<ClusterManager>,
    local_cache: Arc<RwLock<LocalCache>>,
    coherence_directory: Arc<RwLock<CoherenceDirectory>>,
}

/// Coherence directory for tracking cache state
#[derive(Debug, Default)]
pub struct CoherenceDirectory {
    /// Directory entries by key
    entries: HashMap<String, CoherenceState>,
    /// Node membership tracking
    node_cache_entries: HashMap<String, HashSet<String>>,
}

impl DistributedCacheManager {
    /// Create new distributed cache manager
    pub fn new(
        cluster_manager: Arc<ClusterManager>,
        config: CacheConfig,
    ) -> Result<(Self, broadcast::Receiver<InvalidationMessage>), DistributedError> {
        let (invalidation_tx, invalidation_rx) = mpsc::unbounded_channel();
        let local_cache = Arc::new(RwLock::new(LocalCache::default()));
        let metrics = Arc::new(RwLock::new(CacheMetrics::default()));

        let coherence_protocol: Box<dyn CacheCoherenceProtocol> = match config.coherence_protocol {
            CacheCoherenceProtocolType::WriteThrough => {
                Box::new(WriteThroughProtocol::new(
                    cluster_manager.clone(),
                    local_cache.clone()
                ))
            }
            CacheCoherenceProtocolType::Directory => {
                Box::new(DirectoryProtocol::new(
                    cluster_manager.clone(),
                    local_cache.clone()
                ))
            }
            _ => {
                return Err(DistributedError::CacheCoherenceError { 
                    details: "Protocol not yet implemented".to_string() 
                });
            }
        };

        let manager = Self {
            cluster_manager,
            config,
            local_cache,
            coherence_protocol,
            invalidation_tx,
            metrics,
        };

        // Start background tasks
        manager.start_invalidation_processor(invalidation_rx)?;
        manager.start_cache_maintenance()?;

        Ok((manager, broadcast::channel(1000).1))
    }

    /// Get value from cache
    pub async fn get(&self, key: &str) -> Result<Option<CacheValue>, DistributedError> {
        let start_time = SystemTime::now();

        // Check local cache first
        {
            let cache = self.local_cache.read().unwrap();
            if let Some(entry) = cache.get_entry(key) {
                // Update access pattern
                drop(cache);
                self.update_access_pattern(key, true);
                
                // Update metrics
                self.update_cache_metrics(true, false);
                
                info!("Cache hit for key: {}", key);
                return Ok(Some(entry.value.clone()));
            }
        }

        // Cache miss - check coherence protocol
        if self.coherence_protocol.is_valid(key).await.unwrap_or(true) {
            self.update_cache_metrics(false, false);
            info!("Cache miss for key: {}", key);
            Ok(None)
        } else {
            // Entry is invalid, remove it
            self.invalidate_local(key).await?;
            self.update_cache_metrics(false, true);
            Ok(None)
        }
    }

    /// Put value into cache
    pub async fn put(&self, key: &str, value: CacheValue) -> Result<(), DistributedError> {
        info!("Putting key {} into cache", key);

        // Determine appropriate cache level
        let level = self.determine_cache_level(key);

        // Create cache entry
        let entry = CacheEntry {
            key: key.to_string(),
            value: value.clone(),
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 1,
            size: self.estimate_value_size(&value),
            level: level.clone(),
            valid: true,
        };

        // Insert into local cache
        {
            let mut cache = self.local_cache.write().unwrap();
            cache.insert_entry(key, entry, &self.config)?;
        }

        // Handle coherence
        self.coherence_protocol.handle_update(key, &value).await?;

        // Update access pattern
        self.update_access_pattern(key, false);

        info!("Successfully cached key: {}", key);
        Ok(())
    }

    /// Invalidate cache entry
    pub async fn invalidate(&self, key: &str, reason: InvalidationReason) -> Result<(), DistributedError> {
        info!("Invalidating cache key: {}", key);

        // Send invalidation message to cluster
        let message = InvalidationMessage {
            invalidation_id: format!("inv_{}_{}", key, thread_rng().gen::<u32>()),
            cache_key: key.to_string(),
            source_node: "self".to_string(), // TODO: Get actual node ID
            reason,
            timestamp: SystemTime::now(),
            target_nodes: Vec::new(), // Send to all nodes
        };

        // Broadcast invalidation
        if let Err(_) = self.invalidation_tx.send(message.clone()) {
            error!("Failed to broadcast invalidation for key: {}", key);
        }

        // Handle local invalidation
        self.coherence_protocol.handle_invalidation(message).await?;

        Ok(())
    }

    /// Get cache metrics
    pub fn get_metrics(&self) -> CacheMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Get local cache statistics
    pub fn get_cache_stats(&self) -> CacheStats {
        let cache = self.local_cache.read().unwrap();
        CacheStats {
            l1_entries: cache.l1_cache.len(),
            l2_entries: cache.l2_cache.len(),
            l3_entries: cache.l3_cache.len(),
            l1_usage: cache.l1_usage,
            l2_usage: cache.l2_usage,
            l3_usage: cache.l3_usage,
            total_entries: cache.l1_cache.len() + cache.l2_cache.len() + cache.l3_cache.len(),
            total_usage: cache.l1_usage + cache.l2_usage + cache.l3_usage,
        }
    }

    // Private methods

    fn start_invalidation_processor(
        &self,
        mut rx: mpsc::UnboundedReceiver<InvalidationMessage>,
    ) -> Result<(), DistributedError> {
        let coherence_protocol = self.coherence_protocol.clone();
        let local_cache = self.local_cache.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                debug!("Processing invalidation: {}", message.cache_key);

                // Handle coherence protocol
                if let Err(e) = coherence_protocol.handle_invalidation(message.clone()).await {
                    error!("Failed to process invalidation: {}", e);
                }

                // Remove from local cache
                {
                    let mut cache = local_cache.write().unwrap();
                    cache.remove_entry(&message.cache_key);
                }

                // Update metrics
                {
                    let mut m = metrics.write().unwrap();
                    m.invalidations += 1;
                }

                info!("Processed invalidation for key: {}", message.cache_key);
            }
        });

        Ok(())
    }

    fn start_cache_maintenance(&self) -> Result<(), DistributedError> {
        let local_cache = self.local_cache.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(60)); // Run every minute

            loop {
                interval_timer.tick().await;

                let mut promotions = 0;
                let mut demotions = 0;
                let mut evictions = 0;

                // TTL-based eviction
                {
                    let mut cache = local_cache.write().unwrap();
                    let now = SystemTime::now();

                    // Check all cache levels for expired entries
                    let expired_keys: Vec<String> = cache.l1_cache.iter()
                        .chain(cache.l2_cache.iter())
                        .chain(cache.l3_cache.iter())
                        .filter_map(|(k, v)| {
                            if now.duration_since(v.created_at).unwrap_or_default() > config.entry_ttl {
                                Some(k.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    for key in expired_keys {
                        cache.remove_entry(&key);
                        evictions += 1;
                    }

                    // Size-based eviction if needed
                    cache.enforce_size_limits(&config, &mut evictions);
                }

                // Update metrics
                {
                    let mut m = metrics.write().unwrap();
                    m.promotions += promotions;
                    m.demotions += demotions;
                    m.evictions += evictions;
                }

                if evictions > 0 {
                    info!("Cache maintenance: evicted {} entries", evictions);
                }
            }
        });

        Ok(())
    }

    fn determine_cache_level(&self, key: &str) -> CacheLevel {
        // Check access patterns to determine appropriate level
        let cache = self.local_cache.read().unwrap();
        
        if let Some(pattern) = cache.access_patterns.get(key) {
            if pattern.access_frequency > 10.0 { // Hot data
                CacheLevel::L1
            } else if pattern.access_frequency > 1.0 { // Warm data
                CacheLevel::L2
            } else { // Cold data
                CacheLevel::L3
            }
        } else {
            // New entries start in L3
            CacheLevel::L3
        }
    }

    fn update_access_pattern(&self, key: &str, is_hit: bool) {
        let mut cache = self.local_cache.write().unwrap();
        let now = SystemTime::now();
        
        let pattern = cache.access_patterns.entry(key.to_string()).or_default();
        pattern.total_accesses += 1;
        pattern.last_access = Some(now);
        
        // Calculate access frequency (simplified)
        if let Some(last_access) = pattern.last_access {
            let duration = now.duration_since(last_access).unwrap_or_default();
            if duration.as_secs() > 0 {
                pattern.access_frequency = pattern.total_accesses as f64 / duration.as_secs() as f64;
            }
        }
    }

    fn estimate_value_size(&self, value: &CacheValue) -> u64 {
        match value {
            CacheValue::Bytes(bytes) => bytes.len() as u64,
            CacheValue::Vertex(vertex) => {
                // Rough estimation
                std::mem::size_of::<VertexData>() as u64 + 
                vertex.properties.len() as u64 * 50 // Approximate property size
            }
            CacheValue::Edge(edge) => {
                std::mem::size_of::<EdgeData>() as u64 + 
                edge.properties.len() as u64 * 50
            }
            CacheValue::QueryResult(_) => 1024, // Approximate
            CacheValue::Aggregate(_) => 512,     // Approximate
        }
    }

    fn update_cache_metrics(&self, hit: bool, coherence_miss: bool) {
        let mut metrics = self.metrics.write().unwrap();
        
        if hit {
            metrics.total_hits += 1;
        } else {
            metrics.total_misses += 1;
        }
        
        let total = metrics.total_hits + metrics.total_misses;
        if total > 0 {
            metrics.hit_ratio = (metrics.total_hits as f64 / total as f64) * 100.0;
        }
        
        if coherence_miss {
            metrics.coherence_messages += 1;
        }
    }

    async fn invalidate_local(&self, key: &str) -> Result<(), DistributedError> {
        let mut cache = self.local_cache.write().unwrap();
        cache.remove_entry(key);
        Ok(())
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub l1_entries: usize,
    pub l2_entries: usize,
    pub l3_entries: usize,
    pub l1_usage: u64,
    pub l2_usage: u64,
    pub l3_usage: u64,
    pub total_entries: usize,
    pub total_usage: u64,
}

impl LocalCache {
    fn get_entry(&self, key: &str) -> Option<CacheEntry> {
        self.l1_cache.get(key)
            .or_else(|| self.l2_cache.get(key))
            .or_else(|| self.l3_cache.get(key))
            .cloned()
    }

    fn insert_entry(&mut self, key: &str, entry: CacheEntry, config: &CacheConfig) -> Result<(), DistributedError> {
        // Remove existing entry if present
        self.remove_entry(key);

        // Insert into appropriate level
        let size = entry.size;
        match entry.level {
            CacheLevel::L1 => {
                if self.l1_usage + size > config.l1_cache_size {
                    // Evict from L1 to make space
                    self.evict_from_l1(config);
                }
                self.l1_cache.insert(key.to_string(), entry);
                self.l1_usage += size;
            }
            CacheLevel::L2 => {
                if self.l2_usage + size > config.l2_cache_size {
                    self.evict_from_l2(config);
                }
                self.l2_cache.insert(key.to_string(), entry);
                self.l2_usage += size;
            }
            CacheLevel::L3 => {
                if self.l3_usage + size > config.l3_cache_size {
                    self.evict_from_l3(config);
                }
                self.l3_cache.insert(key.to_string(), entry);
                self.l3_usage += size;
            }
        }

        Ok(())
    }

    fn remove_entry(&mut self, key: &str) {
        if let Some(entry) = self.l1_cache.remove(key) {
            self.l1_usage -= entry.size;
        } else if let Some(entry) = self.l2_cache.remove(key) {
            self.l2_usage -= entry.size;
        } else if let Some(entry) = self.l3_cache.remove(key) {
            self.l3_usage -= entry.size;
        }
        
        self.access_patterns.remove(key);
    }

    fn evict_from_l1(&mut self, config: &CacheConfig) {
        if let Some((key, entry)) = self.l1_cache.iter().next() {
            let key = key.clone();
            let entry = entry.clone();
            
            // Demote to L2
            self.l1_usage -= entry.size;
            self.l1_cache.remove(&key);
            
            // Try to promote to L2
            if self.l2_usage + entry.size <= config.l2_cache_size {
                self.l2_cache.insert(key.clone(), entry);
                self.l2_usage += entry.size;
            }
        }
    }

    fn evict_from_l2(&mut self, config: &CacheConfig) {
        if let Some((key, entry)) = self.l2_cache.iter().next() {
            let key = key.clone();
            let entry = entry.clone();
            
            // Demote to L3
            self.l2_usage -= entry.size;
            self.l2_cache.remove(&key);
            
            // Try to promote to L3
            if self.l3_usage + entry.size <= config.l3_cache_size {
                self.l3_cache.insert(key.clone(), entry);
                self.l3_usage += entry.size;
            }
        }
    }

    fn evict_from_l3(&mut self, _config: &CacheConfig) {
        if let Some((key, entry)) = self.l3_cache.iter().next() {
            self.l3_usage -= entry.size;
            self.l3_cache.remove(key);
        }
    }

    fn enforce_size_limits(&mut self, config: &CacheConfig, evictions: &mut u64) {
        // Enforce L1 limit
        while self.l1_usage > config.l1_cache_size && !self.l1_cache.is_empty() {
            self.evict_from_l1(config);
            *evictions += 1;
        }

        // Enforce L2 limit
        while self.l2_usage > config.l2_cache_size && !self.l2_cache.is_empty() {
            self.evict_from_l2(config);
            *evictions += 1;
        }

        // Enforce L3 limit
        while self.l3_usage > config.l3_cache_size && !self.l3_cache.is_empty() {
            self.evict_from_l3(config);
            *evictions += 1;
        }
    }
}

impl WriteThroughProtocol {
    fn new(
        cluster_manager: Arc<ClusterManager>,
        local_cache: Arc<RwLock<LocalCache>>,
    ) -> Self {
        Self {
            cluster_manager,
            local_cache,
        }
    }
}

#[async_trait::async_trait]
impl CacheCoherenceProtocol for WriteThroughProtocol {
    async fn handle_invalidation(&self, message: InvalidationMessage) -> Result<(), DistributedError> {
        debug!("Write-through: handling invalidation for {}", message.cache_key);
        
        // Remove from local cache
        let mut cache = self.local_cache.write().unwrap();
        cache.remove_entry(&message.cache_key);
        
        // TODO: Broadcast to other nodes
        
        Ok(())
    }

    async fn handle_update(&self, key: &str, _value: &CacheValue) -> Result<(), DistributedError> {
        debug!("Write-through: handling update for {}", key);
        
        // In write-through, updates are immediately propagated
        // TODO: Implement actual propagation logic
        
        Ok(())
    }

    async fn is_valid(&self, key: &str) -> Result<bool, DistributedError> {
        let cache = self.local_cache.read().unwrap();
        Ok(cache.get_entry(key).map_or(false, |e| e.valid))
    }

    async fn get_coherence_state(&self, key: &str) -> Result<CoherenceState, DistributedError> {
        Ok(CoherenceState {
            key: key.to_string(),
            owner_node: "unknown".to_string(), // TODO: Get actual node ID
            valid_nodes: Vec::new(),
            invalidated_nodes: Vec::new(),
            last_modified: SystemTime::now(),
            version: 1,
        })
    }
}

/// Distributed transaction coordinator
#[derive(Debug)]
pub struct TransactionCoordinator {
    /// Cluster manager reference
    cluster_manager: Arc<ClusterManager>,
    /// Active transactions
    active_transactions: Arc<RwLock<HashMap<String, DistributedTransaction>>>,
    /// Transaction configuration
    config: TransactionConfig,
    /// Transaction log
    transaction_log: Arc<RwLock<TransactionLog>>,
    /// Transaction metrics
    metrics: Arc<RwLock<TransactionMetrics>>,
}

/// Transaction configuration
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Transaction timeout
    pub transaction_timeout: Duration,
    /// Maximum retry attempts
    pub max_retries: u8,
    /// Enable distributed locking
    pub enable_distributed_locking: bool,
    /// Lock timeout
    pub lock_timeout: Duration,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// Enable optimistic concurrency control
    pub enable_optimistic_concurrency: bool,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            transaction_timeout: Duration::from_secs(30),
            max_retries: 3,
            enable_distributed_locking: true,
            lock_timeout: Duration::from_secs(10),
            isolation_level: IsolationLevel::ReadCommitted,
            enable_optimistic_concurrency: false,
        }
    }
}

/// Isolation levels for transactions
#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    /// Read uncommitted (lowest isolation)
    ReadUncommitted,
    /// Read committed (default)
    ReadCommitted,
    /// Repeatable read
    RepeatableRead,
    /// Serializable (highest isolation)
    Serializable,
}

/// Transaction log for durability
#[derive(Debug, Default)]
pub struct TransactionLog {
    /// Log entries
    entries: Vec<TransactionLogEntry>,
    /// Log sequence number
    lsn: u64,
}

/// Transaction log entry
#[derive(Debug, Clone)]
pub struct TransactionLogEntry {
    /// Log sequence number
    pub lsn: u64,
    /// Transaction ID
    pub transaction_id: String,
    /// Entry type
    pub entry_type: LogEntryType,
    /// Operation data
    pub operation: Option<TransactionOperation>,
    /// Timestamp
    pub timestamp: SystemTime,
    /// Node ID
    pub node_id: String,
}

/// Log entry types
#[derive(Debug, Clone, PartialEq)]
pub enum LogEntryType {
    /// Transaction start
    Begin,
    /// Operation prepared
    Prepare,
    /// Transaction committed
    Commit,
    /// Transaction aborted
    Abort,
    /// Checkpoint
    Checkpoint,
}

/// Transaction metrics
#[derive(Debug, Clone, Default)]
pub struct TransactionMetrics {
    /// Total transactions
    pub total_transactions: u64,
    /// Committed transactions
    pub committed_transactions: u64,
    /// Aborted transactions
    pub aborted_transactions: u64,
    /// Average transaction duration
    pub avg_duration_ms: f64,
    /// Concurrent transaction peak
    pub concurrent_peak: u32,
    /// Lock conflicts
    pub lock_conflicts: u64,
    /// Deadlocks
    pub deadlocks: u64,
    /// Retries due to conflicts
    pub retries: u64,
}

/// Transaction execution result
#[derive(Debug, Clone)]
pub struct TransactionResult {
    /// Transaction ID
    pub transaction_id: String,
    /// Execution status
    pub status: TransactionPhase,
    /// Committed operations
    pub committed_operations: Vec<TransactionOperation>,
    /// Execution duration
    pub duration_ms: u64,
    /// Error if failed
    pub error: Option<String>,
    /// Participants
    pub participants: Vec<String>,
}

impl TransactionCoordinator {
    /// Create new transaction coordinator
    pub fn new(cluster_manager: Arc<ClusterManager>) -> Self {
        Self::with_config(cluster_manager, TransactionConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(cluster_manager: Arc<ClusterManager>, config: TransactionConfig) -> Self {
        Self {
            cluster_manager,
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            config,
            transaction_log: Arc::new(RwLock::new(TransactionLog::default())),
            metrics: Arc::new(RwLock::new(TransactionMetrics::default())),
        }
    }

    /// Begin a new distributed transaction
    pub async fn begin_transaction(
        &self,
        participants: Vec<String>,
        operations: Vec<TransactionOperation>,
    ) -> Result<String, DistributedError> {
        let transaction_id = self.generate_transaction_id();
        
        info!("Beginning distributed transaction: {}", transaction_id);

        // Create transaction context
        let transaction = DistributedTransaction {
            transaction_id: transaction_id.clone(),
            coordinator: "self".to_string(), // TODO: Get actual node ID
            participants: participants.clone(),
            phase: TransactionPhase::Preparing,
            prepared_operations: operations,
            commit_deadline: SystemTime::now() + self.config.transaction_timeout,
        };

        // Log transaction start
        self.log_transaction_event(&transaction_id, LogEntryType::Begin, None).await?;

        // Add to active transactions
        {
            let mut active = self.active_transactions.write().unwrap();
            active.insert(transaction_id.clone(), transaction);
        }

        // Update metrics
        self.update_transaction_metrics(|metrics| {
            metrics.total_transactions += 1;
            let current = active.read().unwrap().len() as u32;
            if current > metrics.concurrent_peak {
                metrics.concurrent_peak = current;
            }
        });

        info!("Transaction {} created with {} participants", transaction_id, participants.len());
        Ok(transaction_id)
    }

    /// Execute transaction using two-phase commit
    pub async fn execute_transaction(&self, transaction_id: &str) -> Result<TransactionResult, DistributedError> {
        let start_time = SystemTime::now();
        info!("Executing distributed transaction: {}", transaction_id);

        // Get transaction context
        let transaction = {
            let active = self.active_transactions.read().unwrap();
            active.get(transaction_id).cloned()
                .ok_or_else(|| DistributedError::TransactionFailed { 
                    transaction_id: transaction_id.to_string() 
                })?
        };

        // Phase 1: Prepare
        info!("Starting Phase 1: Prepare for transaction {}", transaction_id);
        let prepare_results = self.execute_prepare_phase(&transaction).await?;

        // Check if all participants prepared successfully
        let all_prepared = prepare_results.iter().all(|(node, result)| {
            result.is_ok()
        });

        if !all_prepared {
            // Phase 1 failed -> Abort
            info!("Phase 1 failed for transaction {}, initiating abort", transaction_id);
            self.execute_abort_phase(&transaction, &prepare_results).await?;
            
            let duration = start_time.duration_since(SystemTime::now()).unwrap_or_default();
            return Ok(TransactionResult {
                transaction_id: transaction_id.to_string(),
                status: TransactionPhase::Aborted,
                committed_operations: Vec::new(),
                duration_ms: duration.as_millis() as u64,
                error: Some("Prepare phase failed".to_string()),
                participants: transaction.participants,
            });
        }

        // Phase 2: Commit
        info!("Starting Phase 2: Commit for transaction {}", transaction_id);
        let commit_results = self.execute_commit_phase(&transaction).await?;

        // Verify all commits succeeded
        let all_committed = commit_results.iter().all(|(node, result)| {
            result.is_ok()
        });

        if !all_committed {
            error!("Critical error: Some participants failed to commit for transaction {}", transaction_id);
            // This is a severe error - data inconsistency may have occurred
            return Err(DistributedError::TransactionFailed { 
                transaction_id: transaction_id.to_string() 
            });
        }

        // Transaction completed successfully
        info!("Transaction {} completed successfully", transaction_id);

        // Log commit
        self.log_transaction_event(transaction_id, LogEntryType::Commit, None).await?;

        // Remove from active transactions
        {
            let mut active = self.active_transactions.write().unwrap();
            active.remove(transaction_id);
        }

        // Update metrics
        let duration = start_time.duration_since(SystemTime::now()).unwrap_or_default();
        self.update_transaction_metrics(|metrics| {
            metrics.committed_transactions += 1;
            let total_time = metrics.avg_duration_ms * (metrics.committed_transactions - 1) as f64;
            metrics.avg_duration_ms = (total_time + duration.as_millis() as f64) / metrics.committed_transactions as f64;
        });

        Ok(TransactionResult {
            transaction_id: transaction_id.to_string(),
            status: TransactionPhase::Committed,
            committed_operations: transaction.prepared_operations,
            duration_ms: duration.as_millis() as u64,
            error: None,
            participants: transaction.participants,
        })
    }

    /// Abort an active transaction
    pub async fn abort_transaction(&self, transaction_id: &str) -> Result<(), DistributedError> {
        info!("Aborting transaction: {}", transaction_id);

        let transaction = {
            let active = self.active_transactions.read().unwrap();
            active.get(transaction_id).cloned()
                .ok_or_else(|| DistributedError::TransactionFailed { 
                    transaction_id: transaction_id.to_string() 
                })?
        };

        // Execute abort phase
        let empty_results = HashMap::new();
        self.execute_abort_phase(&transaction, &empty_results).await?;

        // Log abort
        self.log_transaction_event(transaction_id, LogEntryType::Abort, None).await?;

        // Remove from active transactions
        {
            let mut active = self.active_transactions.write().unwrap();
            active.remove(transaction_id);
        }

        // Update metrics
        self.update_transaction_metrics(|metrics| {
            metrics.aborted_transactions += 1;
        });

        Ok(())
    }

    /// Get transaction metrics
    pub fn get_metrics(&self) -> TransactionMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Get active transactions
    pub fn get_active_transactions(&self) -> Vec<DistributedTransaction> {
        self.active_transactions.read().unwrap().values().cloned().collect()
    }

    /// Clean up expired transactions
    pub async fn cleanup_expired_transactions(&self) -> Result<u32, DistributedError> {
        let now = SystemTime::now();
        let mut expired_transactions = Vec::new();

        {
            let active = self.active_transactions.read().unwrap();
            for (id, transaction) in active.iter() {
                if transaction.commit_deadline <= now {
                    expired_transactions.push(id.clone());
                }
            }
        }

        let cleaned_count = expired_transactions.len() as u32;

        for transaction_id in expired_transactions {
            warn!("Cleaning up expired transaction: {}", transaction_id);
            if let Err(e) = self.abort_transaction(&transaction_id).await {
                error!("Failed to abort expired transaction {}: {}", transaction_id, e);
            }
        }

        if cleaned_count > 0 {
            info!("Cleaned up {} expired transactions", cleaned_count);
        }

        Ok(cleaned_count)
    }

    // Private methods

    async fn execute_prepare_phase(
        &self,
        transaction: &DistributedTransaction,
    ) -> Result<HashMap<String, Result<(), DistributedError>>, DistributedError> {
        let mut prepare_results = HashMap::new();
        let mut futures = Vec::new();

        // Send prepare to all participants
        for participant in &transaction.participants {
            let future = self.send_prepare_to_node(transaction, participant);
            futures.push((participant.clone(), future));
        }

        // Wait for all prepare responses
        for (participant, future) in futures {
            let result = future.await;
            prepare_results.insert(participant, result);
        }

        // Log prepare phase
        self.log_transaction_event(&transaction.transaction_id, LogEntryType::Prepare, None).await?;

        Ok(prepare_results)
    }

    async fn execute_commit_phase(
        &self,
        transaction: &DistributedTransaction,
    ) -> Result<HashMap<String, Result<(), DistributedError>>, DistributedError> {
        let mut commit_results = HashMap::new();
        let mut futures = Vec::new();

        // Send commit to all participants
        for participant in &transaction.participants {
            let future = self.send_commit_to_node(transaction, participant);
            futures.push((participant.clone(), future));
        }

        // Wait for all commit responses
        for (participant, future) in futures {
            let result = future.await;
            commit_results.insert(participant, result);
        }

        Ok(commit_results)
    }

    async fn execute_abort_phase(
        &self,
        transaction: &DistributedTransaction,
        _prepare_results: &HashMap<String, Result<(), DistributedError>>,
    ) -> Result<(), DistributedError> {
        let mut futures = Vec::new();

        // Send abort to all participants that prepared successfully
        for participant in &transaction.participants {
            let future = self.send_abort_to_node(transaction, participant);
            futures.push(future);
        }

        // Wait for all abort responses
        let results = futures::future::join_all(futures).await;

        // Check if all aborts succeeded
        for (i, result) in results.into_iter().enumerate() {
            if let Err(e) = result {
                error!("Failed to abort transaction {} on node {}: {}", 
                       transaction.transaction_id, 
                       transaction.participants.get(i).unwrap_or(&"unknown".to_string()), 
                       e);
            }
        }

        Ok(())
    }

    async fn send_prepare_to_node(
        &self,
        transaction: &DistributedTransaction,
        node_id: &str,
    ) -> Result<(), DistributedError> {
        debug!("Sending prepare to node {} for transaction {}", node_id, transaction.transaction_id);

        // TODO: Implement actual RPC to node
        // For now, simulate success/failure based on load
        let topology = self.cluster_manager.get_topology();
        if let Some(node) = topology.nodes.get(node_id) {
            if node.load_factor > 0.9 {
                Err(DistributedError::ConnectionFailed { 
                    node_id: node_id.to_string(), 
                    reason: "Node overloaded".to_string() 
                })
            } else {
                // Simulate some random failures for testing
                if thread_rng().gen_range(0..100) < 5 { // 5% failure rate
                    Err(DistributedError::TransactionFailed { 
                        transaction_id: transaction.transaction_id.clone() 
                    })
                } else {
                    Ok(())
                }
            }
        } else {
            Err(DistributedError::NodeNotFound { 
                node_id: node_id.to_string() 
            })
        }
    }

    async fn send_commit_to_node(
        &self,
        transaction: &DistributedTransaction,
        node_id: &str,
    ) -> Result<(), DistributedError> {
        debug!("Sending commit to node {} for transaction {}", node_id, transaction.transaction_id);

        // TODO: Implement actual RPC to node
        // For now, simulate success
        Ok(())
    }

    async fn send_abort_to_node(
        &self,
        transaction: &DistributedTransaction,
        node_id: &str,
    ) -> Result<(), DistributedError> {
        debug!("Sending abort to node {} for transaction {}", node_id, transaction.transaction_id);

        // TODO: Implement actual RPC to node
        // For now, simulate success
        Ok(())
    }

    fn generate_transaction_id(&self) -> String {
        format!("tx_{}_{}", 
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
                thread_rng().gen::<u32>())
    }

    async fn log_transaction_event(
        &self,
        transaction_id: &str,
        entry_type: LogEntryType,
        operation: Option<TransactionOperation>,
    ) -> Result<(), DistributedError> {
        let mut log = self.transaction_log.write().unwrap();
        
        let entry = TransactionLogEntry {
            lsn: log.lsn,
            transaction_id: transaction_id.to_string(),
            entry_type,
            operation,
            timestamp: SystemTime::now(),
            node_id: "self".to_string(), // TODO: Get actual node ID
        };

        log.entries.push(entry);
        log.lsn += 1;

        // TODO: Persist log to durable storage

        Ok(())
    }

    fn update_transaction_metrics<F>(&self, updater: F) 
    where
        F: FnOnce(&mut TransactionMetrics),
    {
        let mut metrics = self.metrics.write().unwrap();
        updater(&mut metrics);
    }
}

/// Fault detection and auto-recovery system
#[derive(Debug)]
pub struct FaultDetector {
    /// Cluster manager reference
    cluster_manager: Arc<ClusterManager>,
    /// Detector configuration
    config: FaultDetectorConfig,
    /// Fault detection rules
    detection_rules: Vec<Box<dyn FaultDetectionRule>>,
    /// Recovery strategies
    recovery_strategies: HashMap<FaultType, Box<dyn RecoveryStrategy>>,
    /// Fault history
    fault_history: Arc<RwLock<Vec<FaultEvent>>>,
    /// Current cluster health
    cluster_health: Arc<RwLock<ClusterHealth>>,
    /// Recovery metrics
    recovery_metrics: Arc<RwLock<RecoveryMetrics>>,
}

/// Fault detector configuration
#[derive(Debug, Clone)]
pub struct FaultDetectorConfig {
    /// Health check interval
    pub health_check_interval: Duration,
    /// Node timeout threshold
    pub node_timeout: Duration,
    /// Maximum consecutive failures before marking as failed
    pub max_consecutive_failures: u8,
    /// Enable automatic recovery
    pub enable_auto_recovery: bool,
    /// Recovery delay after fault detection
    pub recovery_delay: Duration,
    /// Enable network partition detection
    pub enable_partition_detection: bool,
    /// Enable performance degradation detection
    pub enable_performance_monitoring: bool,
    /// Performance threshold for degradation
    pub performance_threshold: f64,
}

impl Default for FaultDetectorConfig {
    fn default() -> Self {
        Self {
            health_check_interval: Duration::from_secs(5),
            node_timeout: Duration::from_secs(15),
            max_consecutive_failures: 3,
            enable_auto_recovery: true,
            recovery_delay: Duration::from_secs(10),
            enable_partition_detection: true,
            enable_performance_monitoring: true,
            performance_threshold: 0.8, // 80% load threshold
        }
    }
}

/// Fault detection rule trait
#[async_trait::async_trait]
pub trait FaultDetectionRule: Send + Sync + std::fmt::Debug {
    /// Detect faults based on current cluster state
    async fn detect_faults(&self, cluster_state: &ClusterTopology) -> Vec<FaultType>;
    
    /// Get rule name
    fn name(&self) -> &str;
}

/// Recovery strategy trait
#[async_trait::async_trait]
pub trait RecoveryStrategy: Send + Sync + std::fmt::Debug {
    /// Execute recovery for a specific fault
    async fn recover(&self, fault: &FaultEvent, cluster_manager: &ClusterManager) -> Result<(), DistributedError>;
    
    /// Get strategy name
    fn name(&self) -> &str;
    
    /// Check if this strategy can handle the fault type
    fn can_handle(&self, fault_type: &FaultType) -> bool;
}

/// Recovery metrics
#[derive(Debug, Clone, Default)]
pub struct RecoveryMetrics {
    /// Total faults detected
    pub total_faults: u64,
    /// Successful recoveries
    pub successful_recoveries: u64,
    /// Failed recoveries
    pub failed_recoveries: u64,
    /// Average recovery time
    pub avg_recovery_time_ms: f64,
    /// Active recovery operations
    pub active_recoveries: u32,
    /// Recovery success rate
    pub success_rate: f64,
}

/// Node failure detection rule
#[derive(Debug)]
pub struct NodeFailureRule {
    timeout: Duration,
    max_failures: u8,
}

/// Network partition detection rule
#[derive(Debug)]
pub struct NetworkPartitionRule {
    min_nodes_for_partition: u8,
}

/// Performance degradation detection rule
#[derive(Debug)]
pub struct PerformanceDegradationRule {
    load_threshold: f64,
    latency_threshold: u64,
}

/// Node failure recovery strategy
#[derive(Debug)]
pub struct NodeFailureRecovery {
    retry_attempts: u8,
    retry_delay: Duration,
    enable_replacement: bool,
}

/// Network partition recovery strategy
#[derive(Debug)]
pub struct NetworkPartitionRecovery {
    healing_timeout: Duration,
    min_nodes_for_quorum: u8,
}

/// Performance recovery strategy
#[derive(Debug)]
pub struct PerformanceRecovery {
    enable_load_balancing: bool,
    enable_scaling: bool,
    scale_threshold: f64,
}

impl FaultDetector {
    /// Create new fault detector
    pub fn new(cluster_manager: Arc<ClusterManager>) -> Self {
        Self::with_config(cluster_manager, FaultDetectorConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(cluster_manager: Arc<ClusterManager>, config: FaultDetectorConfig) -> Self {
        let mut detection_rules: Vec<Box<dyn FaultDetectionRule>> = Vec::new();
        
        // Add default detection rules
        detection_rules.push(Box::new(NodeFailureRule {
            timeout: config.node_timeout,
            max_failures: config.max_consecutive_failures,
        }));
        
        if config.enable_partition_detection {
            detection_rules.push(Box::new(NetworkPartitionRule {
                min_nodes_for_partition: 3,
            }));
        }
        
        if config.enable_performance_monitoring {
            detection_rules.push(Box::new(PerformanceDegradationRule {
                load_threshold: config.performance_threshold,
                latency_threshold: 1000, // 1 second
            }));
        }

        let mut recovery_strategies: HashMap<FaultType, Box<dyn RecoveryStrategy>> = HashMap::new();
        
        // Add default recovery strategies
        recovery_strategies.insert(
            FaultType::NodeFailure("".to_string()),
            Box::new(NodeFailureRecovery {
                retry_attempts: 3,
                retry_delay: Duration::from_secs(2),
                enable_replacement: true,
            })
        );
        
        recovery_strategies.insert(
            FaultType::NetworkPartition(Vec::new()),
            Box::new(NetworkPartitionRecovery {
                healing_timeout: Duration::from_secs(60),
                min_nodes_for_quorum: 2,
            })
        );
        
        recovery_strategies.insert(
            FaultType::PerformanceDegradation("".to_string()),
            Box::new(PerformanceRecovery {
                enable_load_balancing: true,
                enable_scaling: true,
                scale_threshold: 0.9,
            })
        );

        Self {
            cluster_manager,
            config,
            detection_rules,
            recovery_strategies,
            fault_history: Arc::new(RwLock::new(Vec::new())),
            cluster_health: Arc::new(RwLock::new(ClusterHealth::Healthy)),
            recovery_metrics: Arc::new(RwLock::new(RecoveryMetrics::default())),
        }
    }

    /// Start fault detection monitoring
    pub async fn start_monitoring(&self) -> Result<(), DistributedError> {
        info!("Starting fault detection monitoring");

        let cluster_manager = self.cluster_manager.clone();
        let detection_rules = self.detection_rules.clone();
        let recovery_strategies = self.recovery_strategies.clone();
        let fault_history = self.fault_history.clone();
        let cluster_health = self.cluster_health.clone();
        let recovery_metrics = self.recovery_metrics.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut interval_timer = interval(config.health_check_interval);

            loop {
                interval_timer.tick().await;

                let cluster_state = cluster_manager.get_topology();
                let mut detected_faults = Vec::new();

                // Run all detection rules
                for rule in &detection_rules {
                    let rule_faults = rule.detect_faults(&cluster_state).await;
                    for fault_type in rule_faults {
                        if !detected_faults.contains(&fault_type) {
                            detected_faults.push(fault_type);
                        }
                    }
                }

                // Process detected faults
                for fault_type in detected_faults {
                    if let Err(e) = Self::process_fault(
                        &fault_type,
                        &cluster_manager,
                        &recovery_strategies,
                        &fault_history,
                        &cluster_health,
                        &recovery_metrics,
                        &config,
                    ).await {
                        error!("Failed to process fault {:?}: {}", fault_type, e);
                    }
                }
            }
        });

        Ok(())
    }

    /// Get current cluster health
    pub fn get_cluster_health(&self) -> ClusterHealth {
        self.cluster_health.read().unwrap().clone()
    }

    /// Get fault history
    pub fn get_fault_history(&self) -> Vec<FaultEvent> {
        self.fault_history.read().unwrap().clone()
    }

    /// Get recovery metrics
    pub fn get_recovery_metrics(&self) -> RecoveryMetrics {
        self.recovery_metrics.read().unwrap().clone()
    }

    /// Manually trigger recovery for a fault
    pub async fn trigger_recovery(&self, fault_type: FaultType) -> Result<(), DistributedError> {
        let fault_event = FaultEvent {
            event_id: format!("manual_{}", thread_rng().gen::<u32>()),
            fault_type: fault_type.clone(),
            detected_at: SystemTime::now(),
            affected_nodes: Vec::new(), // Will be filled by recovery strategy
            recovery_actions: Vec::new(),
            resolved_at: None,
        };

        Self::process_fault(
            &fault_type,
            &self.cluster_manager,
            &self.recovery_strategies,
            &self.fault_history,
            &self.cluster_health,
            &self.recovery_metrics,
            &self.config,
        ).await
    }

    // Private static methods

    async fn process_fault(
        fault_type: &FaultType,
        cluster_manager: &Arc<ClusterManager>,
        recovery_strategies: &HashMap<FaultType, Box<dyn RecoveryStrategy>>,
        fault_history: &Arc<RwLock<Vec<FaultEvent>>>,
        cluster_health: &Arc<RwLock<ClusterHealth>>,
        recovery_metrics: &Arc<RwLock<RecoveryMetrics>>,
        config: &FaultDetectorConfig,
    ) -> Result<(), DistributedError> {
        info!("Processing fault: {:?}", fault_type);

        // Create fault event
        let fault_event = FaultEvent {
            event_id: format!("fault_{}", thread_rng().gen::<u32>()),
            fault_type: fault_type.clone(),
            detected_at: SystemTime::now(),
            affected_nodes: Self::get_affected_nodes(fault_type, cluster_manager),
            recovery_actions: Vec::new(),
            resolved_at: None,
        };

        // Add to fault history
        {
            let mut history = fault_history.write().unwrap();
            history.push(fault_event.clone());
        }

        // Update cluster health
        {
            let mut health = cluster_health.write().unwrap();
            *health = Self::calculate_cluster_health(fault_type, cluster_manager);
        }

        // Trigger recovery if enabled
        if config.enable_auto_recovery {
            // Find appropriate recovery strategy
            let strategy = recovery_strategies.iter()
                .find(|(_, s)| s.can_handle(fault_type));

            if let Some((_, strategy)) = strategy {
                info!("Starting recovery for fault: {:?}", fault_type);
                
                let recovery_start = SystemTime::now();
                
                // Add to active recoveries
                {
                    let mut metrics = recovery_metrics.write().unwrap();
                    metrics.active_recoveries += 1;
                }

                // Execute recovery with delay
                sleep(config.recovery_delay).await;

                let recovery_result = strategy.recover(&fault_event, cluster_manager).await;
                let recovery_time = recovery_start.duration_since(SystemTime::now()).unwrap_or_default();

                // Update recovery metrics
                {
                    let mut metrics = recovery_metrics.write().unwrap();
                    metrics.active_recoveries -= 1;
                    
                    if recovery_result.is_ok() {
                        metrics.successful_recoveries += 1;
                    } else {
                        metrics.failed_recoveries += 1;
                    }
                    
                    let total_recoveries = metrics.successful_recoveries + metrics.failed_recoveries;
                    if total_recoveries > 0 {
                        metrics.success_rate = (metrics.successful_recoveries as f64 / total_recoveries as f64) * 100.0;
                    }
                    
                    let total_time = metrics.avg_recovery_time_ms * (total_recoveries - 1) as f64;
                    metrics.avg_recovery_time_ms = (total_time + recovery_time.as_millis() as f64) / total_recoveries as f64;
                }

                // Update fault event with recovery result
                {
                    let mut history = fault_history.write().unwrap();
                    if let Some(event) = history.last_mut() {
                        if recovery_result.is_ok() {
                            event.resolved_at = Some(SystemTime::now());
                            event.recovery_actions.push("Auto-recovery successful".to_string());
                        } else {
                            event.recovery_actions.push("Auto-recovery failed".to_string());
                        }
                    }
                }

                match recovery_result {
                    Ok(_) => info!("Successfully recovered from fault: {:?}", fault_type),
                    Err(e) => error!("Failed to recover from fault {:?}: {}", fault_type, e),
                }

                recovery_result
            } else {
                warn!("No recovery strategy found for fault: {:?}", fault_type);
                Ok(())
            }
        } else {
            info!("Auto-recovery disabled, fault detected but not recovered: {:?}", fault_type);
            Ok(())
        }
    }

    fn get_affected_nodes(fault_type: &FaultType, cluster_manager: &Arc<ClusterManager>) -> Vec<String> {
        match fault_type {
            FaultType::NodeFailure(node_id) => vec![node_id.clone()],
            FaultType::NetworkPartition(nodes) => nodes.clone(),
            FaultType::PerformanceDegradation(node_id) => vec![node_id.clone()],
            _ => Vec::new(),
        }
    }

    fn calculate_cluster_health(fault_type: &FaultType, cluster_manager: &Arc<ClusterManager>) -> ClusterHealth {
        let topology = cluster_manager.get_topology();
        let total_nodes = topology.nodes.len();

        match fault_type {
            FaultType::NodeFailure(node_id) => {
                if total_nodes > 1 {
                    ClusterHealth::Degraded { failed_nodes: vec![node_id.clone()] }
                } else {
                    ClusterHealth::Recovering
                }
            }
            FaultType::NetworkPartition(nodes) => {
                if nodes.len() >= total_nodes / 2 {
                    ClusterHealth::Partitioned { 
                        partition_groups: vec![nodes.clone(), 
                            topology.nodes.keys()
                                .filter(|n| !nodes.contains(n))
                                .cloned()
                                .collect()]
                    }
                } else {
                    ClusterHealth::Degraded { failed_nodes: nodes.clone() }
                }
            }
            FaultType::PerformanceDegradation(node_id) => {
                ClusterHealth::Degraded { failed_nodes: vec![node_id.clone()] }
            }
            _ => {
                if total_nodes > 0 {
                    ClusterHealth::Healthy
                } else {
                    ClusterHealth::Recovering
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl FaultDetectionRule for NodeFailureRule {
    async fn detect_faults(&self, cluster_state: &ClusterTopology) -> Vec<FaultType> {
        let mut faults = Vec::new();
        let now = SystemTime::now();

        for (node_id, node) in cluster_state.nodes.iter() {
            if let Some(last_heartbeat) = node.last_heartbeat {
                if now.duration_since(last_heartbeat).unwrap_or_default() > self.timeout {
                    faults.push(FaultType::NodeFailure(node_id.clone()));
                }
            } else {
                // No heartbeat at all
                faults.push(FaultType::NodeFailure(node_id.clone()));
            }
        }

        faults
    }

    fn name(&self) -> &str {
        "NodeFailureRule"
    }
}

#[async_trait::async_trait]
impl FaultDetectionRule for NetworkPartitionRule {
    async fn detect_faults(&self, cluster_state: &ClusterTopology) -> Vec<FaultType> {
        let mut faults = Vec::new();
        let total_nodes = cluster_state.nodes.len();

        if total_nodes >= self.min_nodes_for_partition as usize {
            // Simple heuristic: if many nodes have old heartbeats, assume partition
            let now = SystemTime::now();
            let timeout = Duration::from_secs(10);
            let mut isolated_nodes = Vec::new();

            for (node_id, node) in cluster_state.nodes.iter() {
                if let Some(last_heartbeat) = node.last_heartbeat {
                    if now.duration_since(last_heartbeat).unwrap_or_default() > timeout {
                        isolated_nodes.push(node_id.clone());
                    }
                } else {
                    isolated_nodes.push(node_id.clone());
                }
            }

            if isolated_nodes.len() >= total_nodes / 2 {
                faults.push(FaultType::NetworkPartition(isolated_nodes));
            }
        }

        faults
    }

    fn name(&self) -> &str {
        "NetworkPartitionRule"
    }
}

#[async_trait::async_trait]
impl FaultDetectionRule for PerformanceDegradationRule {
    async fn detect_faults(&self, cluster_state: &ClusterTopology) -> Vec<FaultType> {
        let mut faults = Vec::new();

        for (node_id, node) in cluster_state.nodes.iter() {
            if node.load_factor > self.load_threshold {
                faults.push(FaultType::PerformanceDegradation(node_id.clone()));
            }
        }

        faults
    }

    fn name(&self) -> &str {
        "PerformanceDegradationRule"
    }
}

#[async_trait::async_trait]
impl RecoveryStrategy for NodeFailureRecovery {
    async fn recover(&self, fault: &FaultEvent, _cluster_manager: &ClusterManager) -> Result<(), DistributedError> {
        info!("Executing node failure recovery");

        if let FaultType::NodeFailure(node_id) = &fault.fault_type {
            info!("Attempting to recover failed node: {}", node_id);

            // Try to reconnect with retries
            for attempt in 1..=self.retry_attempts {
                debug!("Reconnection attempt {} for node {}", attempt, node_id);

                // TODO: Implement actual reconnection logic
                // For now, simulate with random success
                if thread_rng().gen_range(0..100) > 70 { // 30% success rate
                    info!("Successfully reconnected to node: {}", node_id);
                    return Ok(());
                }

                sleep(self.retry_delay).await;
            }

            // If reconnection fails, consider node replacement
            if self.enable_replacement {
                info!("Node replacement considered for: {}", node_id);
                // TODO: Implement node replacement logic
            }

            Err(DistributedError::NodeNotFound { 
                node_id: node_id.clone() 
            })
        } else {
            Err(DistributedError::TransactionFailed { 
                transaction_id: "Invalid fault type".to_string() 
            })
        }
    }

    fn name(&self) -> &str {
        "NodeFailureRecovery"
    }

    fn can_handle(&self, fault_type: &FaultType) -> bool {
        matches!(fault_type, FaultType::NodeFailure(_))
    }
}

#[async_trait::async_trait]
impl RecoveryStrategy for NetworkPartitionRecovery {
    async fn recover(&self, fault: &FaultEvent, cluster_manager: &ClusterManager) -> Result<(), DistributedError> {
        info!("Executing network partition recovery");

        if let FaultType::NetworkPartition(affected_nodes) = &fault.fault_type {
            info!("Waiting for partition healing: {} nodes affected", affected_nodes.len());

            // Wait for partition to heal
            let start_time = SystemTime::now();
            
            loop {
                let topology = cluster_manager.get_topology();
                let now = SystemTime::now();
                let timeout = Duration::from_secs(5);
                let mut connected_nodes = Vec::new();

                for node_id in affected_nodes {
                    if let Some(node) = topology.nodes.get(node_id) {
                        if let Some(last_heartbeat) = node.last_heartbeat {
                            if now.duration_since(last_heartbeat).unwrap_or_default() < timeout {
                                connected_nodes.push(node_id.clone());
                            }
                        }
                    }
                }

                // Check if we have quorum
                if connected_nodes.len() >= self.min_nodes_for_quorum as usize {
                    info!("Partition healed with {} nodes reconnected", connected_nodes.len());
                    return Ok(());
                }

                // Check healing timeout
                if now.duration_since(start_time).unwrap_or_default() > self.healing_timeout {
                    warn!("Partition healing timeout, continuing with degraded service");
                    return Ok(());
                }

                sleep(Duration::from_secs(2)).await;
            }
        } else {
            Err(DistributedError::TransactionFailed { 
                transaction_id: "Invalid fault type".to_string() 
            })
        }
    }

    fn name(&self) -> &str {
        "NetworkPartitionRecovery"
    }

    fn can_handle(&self, fault_type: &FaultType) -> bool {
        matches!(fault_type, FaultType::NetworkPartition(_))
    }
}

#[async_trait::async_trait]
impl RecoveryStrategy for PerformanceRecovery {
    async fn recover(&self, fault: &FaultEvent, cluster_manager: &ClusterManager) -> Result<(), DistributedError> {
        info!("Executing performance recovery");

        if let FaultType::PerformanceDegradation(node_id) = &fault.fault_type {
            info!("Addressing performance degradation for node: {}", node_id);

            // Load balancing
            if self.enable_load_balancing {
                info!("Initiating load balancing for degraded node");
                // TODO: Implement load balancing logic
            }

            // Auto-scaling
            if self.enable_scaling {
                info!("Considering auto-scaling due to performance issues");
                // TODO: Implement auto-scaling logic
            }

            Ok(())
        } else {
            Err(DistributedError::TransactionFailed { 
                transaction_id: "Invalid fault type".to_string() 
            })
        }
    }

    fn name(&self) -> &str {
        "PerformanceRecovery"
    }

    fn can_handle(&self, fault_type: &FaultType) -> bool {
        matches!(fault_type, FaultType::PerformanceDegradation(_))
    }
}

impl DirectoryProtocol {
    fn new(
        cluster_manager: Arc<ClusterManager>,
        local_cache: Arc<RwLock<LocalCache>>,
    ) -> Self {
        Self {
            cluster_manager,
            local_cache,
            coherence_directory: Arc::new(RwLock::new(CoherenceDirectory::default())),
        }
    }
}

#[async_trait::async_trait]
impl CacheCoherenceProtocol for DirectoryProtocol {
    async fn handle_invalidation(&self, message: InvalidationMessage) -> Result<(), DistributedError> {
        debug!("Directory: handling invalidation for {}", message.cache_key);
        
        // Update directory
        {
            let mut dir = self.coherence_directory.write().unwrap();
            if let Some(state) = dir.entries.get_mut(&message.cache_key) {
                state.invalidated_nodes.push(message.source_node.clone());
                state.last_modified = message.timestamp;
                state.version += 1;
            }
        }
        
        // Remove from local cache
        let mut cache = self.local_cache.write().unwrap();
        cache.remove_entry(&message.cache_key);
        
        // TODO: Broadcast to other nodes based on directory
        
        Ok(())
    }

    async fn handle_update(&self, key: &str, _value: &CacheValue) -> Result<(), DistributedError> {
        debug!("Directory: handling update for {}", key);
        
        // Update directory with new state
        {
            let mut dir = self.coherence_directory.write().unwrap();
            dir.entries.insert(key.to_string(), CoherenceState {
                key: key.to_string(),
                owner_node: "self".to_string(), // TODO: Get actual node ID
                valid_nodes: vec!["self".to_string()],
                invalidated_nodes: Vec::new(),
                last_modified: SystemTime::now(),
                version: 1,
            });
        }
        
        // TODO: Propagate to other nodes
        
        Ok(())
    }

    async fn is_valid(&self, key: &str) -> Result<bool, DistributedError> {
        let dir = self.coherence_directory.read().unwrap();
        
        if let Some(state) = dir.entries.get(key) {
            // Check if current node is in valid list
            // TODO: Get actual node ID
            Ok(state.valid_nodes.contains(&"self".to_string()))
        } else {
            // No state means no cache entry exists yet
            Ok(true)
        }
    }

    async fn get_coherence_state(&self, key: &str) -> Result<CoherenceState, DistributedError> {
        let dir = self.coherence_directory.read().unwrap();
        
        Ok(dir.entries.get(key).cloned().unwrap_or_else(|| CoherenceState {
            key: key.to_string(),
            owner_node: "unknown".to_string(),
            valid_nodes: Vec::new(),
            invalidated_nodes: Vec::new(),
            last_modified: SystemTime::now(),
            version: 0,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distributed_node_creation() {
        let node = DistributedNode {
            node_id: "node_1".to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            role: NodeRole::Worker,
            load_factor: 0.7,
            capabilities: NodeCapabilities {
                can_process_queries: true,
                can_store_data: false,
                can_serve_cache: true,
                max_connections: 1000,
                protocols: vec!["grpc".to_string(), "http".to_string()],
            },
            last_heartbeat: Some(SystemTime::now()),
            metadata: HashMap::new(),
        };

        assert_eq!(node.node_id, "node_1");
        assert_eq!(node.role, NodeRole::Worker);
        assert!(node.load_factor, 0.7);
    }

    #[test]
    fn test_partition_info() {
        let partition = PartitionInfo {
            partition_id: "partition_1".to_string(),
            responsible_nodes: vec!["node_a".to_string(), "node_b".to_string()],
            partition_range: Some(PartitionRange {
                start: "a".to_string(),
                end: "m".to_string(),
                is_hash_based: false,
            }),
            data_size_bytes: 1024 * 1024, // 1MB
            replication_factor: 3,
        };

        assert_eq!(partition.responsible_nodes.len(), 2);
        assert_eq!(partition.replication_factor, 3);
        assert!(partition.partition_range.is_some());
    }

    #[test]
    fn test_transaction_phases() {
        assert_eq!(TransactionPhase::Preparing, TransactionPhase::Preparing);
        assert_ne!(TransactionPhase::Preparing, TransactionPhase::Committed);
        
        assert_eq!(TransactionPhase::Prepared, TransactionPhase::Prepared);
        assert_ne!(TransactionPhase::Prepared, TransactionPhase::Aborted);
    }
}