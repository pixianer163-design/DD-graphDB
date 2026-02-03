//! Cluster Management Module
//!
//! Provides cluster management, node discovery, and coordination services
//! for the distributed graph database.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::net::SocketAddr;
use tokio::sync::{mpsc, broadcast};
use tracing::{info, warn, error, debug, trace};
use sha2::{Sha256, Digest};
use rand::{thread_rng, Rng};

use crate::{DistributedError, NodeHealth, ClusterHealth, AutoScalingConfig};

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
    /// Cluster name
    pub cluster_name: String,
    /// Maximum cluster size
    pub max_cluster_size: u16,
    /// Minimum cluster size for quorum
    pub min_cluster_size: u8,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            node_timeout: Duration::from_secs(15),
            default_replication_factor: 3,
            max_retries: 3,
            auto_scaling: AutoScalingConfig::default(),
            cluster_name: "graph-cluster".to_string(),
            max_cluster_size: 100,
            min_cluster_size: 3,
        }
    }
}

/// Cluster state information
#[derive(Debug, Clone)]
pub struct ClusterState {
    /// Cluster ID
    pub cluster_id: String,
    /// Current leader
    pub leader_id: Option<String>,
    /// Node roles
    pub node_roles: HashMap<String, NodeRole>,
    /// Cluster health
    pub health: ClusterHealth,
    /// Term number for consensus
    pub term: u64,
    /// Last state change
    pub last_state_change: SystemTime,
}

/// Node role in cluster
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRole {
    Leader,
    Follower,
    Candidate,
    Coordinator,
    Worker,
}

/// Node information
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Unique node identifier
    pub node_id: String,
    /// Network address
    pub address: SocketAddr,
    /// Node role
    pub role: NodeRole,
    /// Current load (0.0-1.0)
    pub load_factor: f64,
    /// Supported capabilities
    pub capabilities: NodeCapabilities,
    /// Last heartbeat
    pub last_heartbeat: Option<SystemTime>,
    /// Node metadata
    pub metadata: HashMap<String, String>,
    /// Health status
    pub health: NodeHealth,
    /// Join timestamp
    pub joined_at: SystemTime,
}

/// Node capabilities
#[derive(Debug, Clone)]
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
    /// Memory capacity (bytes)
    pub memory_capacity: u64,
    /// Storage capacity (bytes)
    pub storage_capacity: u64,
}

/// Cluster statistics
#[derive(Debug, Clone, Default)]
pub struct ClusterStats {
    /// Total nodes
    pub total_nodes: u16,
    /// Active nodes
    pub active_nodes: u16,
    /// Failed nodes
    pub failed_nodes: u16,
    /// Average load factor
    pub avg_load_factor: f64,
    /// Total memory capacity
    pub total_memory: u64,
    /// Total storage capacity
    pub total_storage: u64,
    /// Network latency (average ms)
    pub avg_network_latency: u64,
    /// Last rebalance time
    pub last_rebalance: Option<SystemTime>,
}

/// Main cluster manager
#[derive(Debug)]
pub struct ClusterManager {
    /// Current node ID
    pub node_id: String,
    /// Cluster configuration
    pub config: ClusterConfig,
    /// Current cluster state
    pub state: Arc<RwLock<ClusterState>>,
    /// Node information
    pub nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
    /// Cluster metrics
    pub stats: Arc<RwLock<ClusterStats>>,
    /// Event broadcaster
    pub event_tx: broadcast::Sender<ClusterEvent>,
    /// Auto-scaling manager
    pub auto_scaler: Option<Arc<AutoScaler>>,
}

/// Cluster events
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    NodeJoined { node_id: String, node: NodeInfo },
    NodeLeft { node_id: String, reason: String },
    NodeFailed { node_id: String, last_seen: SystemTime },
    LeaderElected { leader_id: String, term: u64 },
    PartitionDetected { affected_nodes: Vec<String> },
    PartitionHealed { recovered_nodes: Vec<String> },
    LoadImbalance { overloaded_nodes: Vec<String>, underloaded_nodes: Vec<String> },
    AutoScalingStarted { target_size: u16 },
    AutoScalingCompleted { old_size: u16, new_size: u16 },
}

/// Auto-scaling manager
#[derive(Debug)]
pub struct AutoScaler {
    /// Cluster configuration
    config: AutoScalingConfig,
    /// Current target size
    target_size: Arc<RwLock<u16>>,
    /// Last scaling action
    last_scaling: Arc<RwLock<Option<SystemTime>>>,
    /// Load history
    load_history: Arc<RwLock<Vec<f64>>>,
}

impl ClusterManager {
    /// Create new cluster manager
    pub fn new(node_id: String, config: ClusterConfig) -> Result<(Self, broadcast::Receiver<ClusterEvent>), DistributedError> {
        let (event_tx, event_rx) = broadcast::channel(1000);
        
        let cluster_id = generate_cluster_id(&node_id, &config.cluster_name);
        
        let state = Arc::new(RwLock::new(ClusterState {
            cluster_id,
            leader_id: None,
            node_roles: HashMap::new(),
            health: ClusterHealth::Healthy,
            term: 0,
            last_state_change: SystemTime::now(),
        }));

        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let stats = Arc::new(RwLock::new(ClusterStats::default()));

        let auto_scaler = if config.auto_scaling.enabled {
            Some(Arc::new(AutoScaler::new(config.auto_scaling.clone())))
        } else {
            None
        };

        let manager = Self {
            node_id,
            config,
            state,
            nodes,
            stats,
            event_tx,
            auto_scaler,
        };

        Ok((manager, event_rx))
    }

    /// Initialize cluster as the first node
    pub async fn initialize_cluster(&self) -> Result<(), DistributedError> {
        info!("Initializing new cluster with node {}", self.node_id);

        let node_info = NodeInfo {
            node_id: self.node_id.clone(),
            address: "127.0.0.1:50051".parse().unwrap(), // TODO: Get actual address
            role: NodeRole::Leader,
            load_factor: 0.0,
            capabilities: NodeCapabilities::default(),
            last_heartbeat: Some(SystemTime::now()),
            metadata: HashMap::new(),
            health: NodeHealth::Healthy,
            joined_at: SystemTime::now(),
        };

        // Add current node
        {
            let mut nodes = self.nodes.write().unwrap();
            nodes.insert(self.node_id.clone(), node_info);
        }

        // Update state
        {
            let mut state = self.state.write().unwrap();
            state.leader_id = Some(self.node_id.clone());
            state.node_roles.insert(self.node_id.clone(), NodeRole::Leader);
        }

        // Update stats
        self.update_stats();

        info!("Cluster initialized with leader {}", self.node_id);
        Ok(())
    }

    /// Join existing cluster
    pub async fn join_cluster(&self, known_nodes: Vec<SocketAddr>) -> Result<(), DistributedError> {
        info!("Joining cluster with {} known nodes", known_nodes.len());

        for addr in known_nodes {
            match self.connect_to_node(addr).await {
                Ok(node_info) => {
                    info!("Connected to node {} at {}", node_info.node_id, addr);
                    self.add_node(node_info).await?;
                    break;
                }
                Err(e) => {
                    warn!("Failed to connect to node at {}: {}", addr, e);
                }
            }
        }

        // Set role as follower initially
        {
            let mut state = self.state.write().unwrap();
            state.node_roles.insert(self.node_id.clone(), NodeRole::Follower);
        }

        Ok(())
    }

    /// Add node to cluster
    pub async fn add_node(&self, node_info: NodeInfo) -> Result<(), DistributedError> {
        let node_id = node_info.node_id.clone();
        
        {
            let mut nodes = self.nodes.write().unwrap();
            nodes.insert(node_id.clone(), node_info.clone());
        }

        // Broadcast node joined event
        let _ = self.event_tx.send(ClusterEvent::NodeJoined { 
            node_id: node_id.clone(), 
            node: node_info 
        });

        info!("Added node {} to cluster", node_id);
        self.update_stats();
        
        Ok(())
    }

    /// Remove node from cluster
    pub async fn remove_node(&self, node_id: &str, reason: &str) -> Result<(), DistributedError> {
        {
            let mut nodes = self.nodes.write().unwrap();
            nodes.remove(node_id);
        }

        {
            let mut state = self.state.write().unwrap();
            state.node_roles.remove(node_id);
            
            // If leader was removed, trigger new election
            if state.leader_id.as_ref() == Some(&node_id.to_string()) {
                state.leader_id = None;
                // TODO: Trigger leader election
            }
        }

        // Broadcast node left event
        let _ = self.event_tx.send(ClusterEvent::NodeLeft { 
            node_id: node_id.to_string(), 
            reason: reason.to_string() 
        });

        info!("Removed node {} from cluster: {}", node_id, reason);
        self.update_stats();
        
        Ok(())
    }

    /// Update node heartbeat
    pub async fn update_heartbeat(&self, node_id: &str, load_factor: f64, health: NodeHealth) -> Result<(), DistributedError> {
        {
            let mut nodes = self.nodes.write().unwrap();
            if let Some(node) = nodes.get_mut(node_id) {
                node.last_heartbeat = Some(SystemTime::now());
                node.load_factor = load_factor;
                node.health = health;
            }
        }

        Ok(())
    }

    /// Get cluster topology
    pub fn get_topology(&self) -> ClusterTopology {
        let nodes = self.nodes.read().unwrap();
        let state = self.state.read().unwrap();
        
        ClusterTopology {
            cluster_id: state.cluster_id.clone(),
            leader_id: state.leader_id.clone(),
            term: state.term,
            nodes: nodes.clone(),
            partitions: Vec::new(), // TODO: Get from partition manager
        }
    }

    /// Get cluster statistics
    pub fn get_stats(&self) -> ClusterStats {
        self.stats.read().unwrap().clone()
    }

    /// Check cluster health
    pub async fn check_health(&self) -> ClusterHealth {
        let now = SystemTime::now();
        let mut failed_nodes = Vec::new();
        
        {
            let nodes = self.nodes.read().unwrap();
            for (node_id, node) in nodes.iter() {
                if let Some(last_heartbeat) = node.last_heartbeat {
                    if now.duration_since(last_heartbeat).unwrap_or(Duration::MAX) > self.config.node_timeout {
                        failed_nodes.push(node_id.clone());
                    }
                } else {
                    failed_nodes.push(node_id.clone());
                }
            }
        }

        if failed_nodes.is_empty() {
            ClusterHealth::Healthy
        } else if failed_nodes.len() < self.nodes.read().unwrap().len() / 2 {
            ClusterHealth::Degraded { failed_nodes }
        } else {
            ClusterHealth::Partitioned { partition_groups: vec![failed_nodes] }
        }
    }

    /// Start background tasks
    pub async fn start_background_tasks(&self) -> Result<(), DistributedError> {
        self.start_health_checker().await?;
        self.start_load_balancer().await?;
        
        if let Some(auto_scaler) = &self.auto_scaler {
            auto_scaler.start_monitoring(
                self.nodes.clone(),
                self.stats.clone(),
                self.event_tx.clone()
            ).await?;
        }

        Ok(())
    }

    // Private methods

    async fn connect_to_node(&self, addr: SocketAddr) -> Result<NodeInfo, DistributedError> {
        // TODO: Implement actual node discovery protocol
        // For now, create mock node
        Ok(NodeInfo {
            node_id: format!("node_{}", thread_rng().gen::<u32>()),
            address: addr,
            role: NodeRole::Leader,
            load_factor: 0.3,
            capabilities: NodeCapabilities::default(),
            last_heartbeat: Some(SystemTime::now()),
            metadata: HashMap::new(),
            health: NodeHealth::Healthy,
            joined_at: SystemTime::now(),
        })
    }

    fn update_stats(&self) {
        let nodes = self.nodes.read().unwrap();
        let mut stats = self.stats.write().unwrap();
        
        stats.total_nodes = nodes.len() as u16;
        stats.active_nodes = nodes.values()
            .filter(|n| matches!(n.health, NodeHealth::Healthy))
            .count() as u16;
        stats.failed_nodes = stats.total_nodes - stats.active_nodes;
        
        if !nodes.is_empty() {
            stats.avg_load_factor = nodes.values()
                .map(|n| n.load_factor)
                .sum::<f64>() / nodes.len() as f64;
        }
        
        stats.total_memory = nodes.values()
            .map(|n| n.capabilities.memory_capacity)
            .sum();
            
        stats.total_storage = nodes.values()
            .map(|n| n.capabilities.storage_capacity)
            .sum();
    }

    async fn start_health_checker(&self) -> Result<(), DistributedError> {
        let nodes = self.nodes.clone();
        let config = self.config.clone();
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            
            loop {
                interval.tick().await;
                
                let now = SystemTime::now();
                let mut failed_nodes = Vec::new();
                
                {
                    let nodes_guard = nodes.read().unwrap();
                    for (node_id, node) in nodes_guard.iter() {
                        if let Some(last_heartbeat) = node.last_heartbeat {
                            if now.duration_since(last_heartbeat).unwrap_or(Duration::MAX) > config.node_timeout {
                                failed_nodes.push((node_id.clone(), *node));
                            }
                        } else {
                            failed_nodes.push((node_id.clone(), *node));
                        }
                    }
                }
                
                for (node_id, node) in failed_nodes {
                    warn!("Node {} failed health check", node_id);
                    let _ = event_tx.send(ClusterEvent::NodeFailed {
                        node_id: node_id.clone(),
                        last_seen: node.last_heartbeat.unwrap_or(now)
                    });
                }
            }
        });

        Ok(())
    }

    async fn start_load_balancer(&self) -> Result<(), DistributedError> {
        let nodes = self.nodes.clone();
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                
                let (overloaded, underloaded) = {
                    let nodes_guard = nodes.read().unwrap();
                    let mut overloaded = Vec::new();
                    let mut underloaded = Vec::new();
                    
                    for (node_id, node) in nodes_guard.iter() {
                        if node.load_factor > 0.8 {
                            overloaded.push(node_id.clone());
                        } else if node.load_factor < 0.3 {
                            underloaded.push(node_id.clone());
                        }
                    }
                    
                    (overloaded, underloaded)
                };
                
                if !overloaded.is_empty() && !underloaded.is_empty() {
                    let _ = event_tx.send(ClusterEvent::LoadImbalance {
                        overloaded_nodes: overloaded,
                        underloaded_nodes: underloaded
                    });
                }
            }
        });

        Ok(())
    }
}

/// Cluster topology information
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    pub cluster_id: String,
    pub leader_id: Option<String>,
    pub term: u64,
    pub nodes: HashMap<String, NodeInfo>,
    pub partitions: Vec<PartitionInfo>,
}

/// Partition information
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub partition_id: String,
    pub node_id: String,
    pub data_size: u64,
    pub key_range: Option<String>,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            can_process_queries: true,
            can_store_data: true,
            can_serve_cache: true,
            max_connections: 1000,
            protocols: vec!["grpc".to_string()],
            memory_capacity: 8 * 1024 * 1024 * 1024, // 8GB
            storage_capacity: 100 * 1024 * 1024 * 1024, // 100GB
        }
    }
}

impl AutoScaler {
    pub fn new(config: AutoScalingConfig) -> Self {
        Self {
            config,
            target_size: Arc::new(RwLock::new(1)),
            last_scaling: Arc::new(RwLock::new(None)),
            load_history: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn start_monitoring(
        &self,
        nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
        stats: Arc<RwLock<ClusterStats>>,
        event_tx: broadcast::Sender<ClusterEvent>,
    ) -> Result<(), DistributedError> {
        let config = self.config.clone();
        let target_size = self.target_size.clone();
        let last_scaling = self.last_scaling.clone();
        let load_history = self.load_history.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.evaluation_window);
            
            loop {
                interval.tick().await;
                
                // Check if we should scale
                if let Some(action) = Self::evaluate_scaling(
                    &config,
                    &nodes,
                    &stats,
                    &load_history,
                    &last_scaling,
                ).await {
                    let current_size = {
                        let nodes_guard = nodes.read().unwrap();
                        nodes_guard.len() as u16
                    };
                    
                    match action {
                        ScalingAction::ScaleUp(target) => {
                            let _ = event_tx.send(ClusterEvent::AutoScalingStarted { target_size: target });
                            // TODO: Implement scale up logic
                            *target_size.write().unwrap() = target;
                            *last_scaling.write().unwrap() = Some(SystemTime::now());
                        }
                        ScalingAction::ScaleDown(target) => {
                            // TODO: Implement scale down logic
                            *target_size.write().unwrap() = target;
                            *last_scaling.write().unwrap() = Some(SystemTime::now());
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn evaluate_scaling(
        config: &AutoScalingConfig,
        nodes: &Arc<RwLock<HashMap<String, NodeInfo>>>,
        stats: &Arc<RwLock<ClusterStats>>,
        load_history: &Arc<RwLock<Vec<f64>>>,
        last_scaling: &Arc<RwLock<Option<SystemTime>>>,
    ) -> Option<ScalingAction> {
        let current_size = {
            let nodes_guard = nodes.read().unwrap();
            nodes_guard.len() as u16
        };

        // Check cooldown period
        if let Some(last) = *last_scaling.read().unwrap() {
            if SystemTime::now().duration_since(last).unwrap_or(Duration::MAX) < config.scaling_cooldown {
                return None;
            }
        }

        let stats_guard = stats.read().unwrap();
        let avg_load = stats_guard.avg_load_factor;

        // Update load history
        {
            let mut history = load_history.write().unwrap();
            history.push(avg_load);
            if history.len() > 10 {
                history.remove(0);
            }
        }

        // Check scaling conditions
        let history_avg = {
            let history = load_history.read().unwrap();
            if history.is_empty() { 0.0 } else { history.iter().sum::<f64>() / history.len() as f64 }
        };

        if history_avg > config.scale_up_threshold && current_size < config.max_nodes {
            Some(ScalingAction::ScaleUp(current_size + 1))
        } else if history_avg < config.scale_down_threshold && current_size > config.min_nodes {
            Some(ScalingAction::ScaleDown(current_size - 1))
        } else {
            None
        }
    }
}

/// Scaling actions
#[derive(Debug)]
enum ScalingAction {
    ScaleUp(u16),
    ScaleDown(u16),
}

/// Generate unique cluster ID
fn generate_cluster_id(node_id: &str, cluster_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(node_id.as_bytes());
    hasher.update(cluster_name.as_bytes());
    hasher.update(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().to_string().as_bytes());
    format!("cluster_{:x}", hasher.finalize())[..16].to_string()
}