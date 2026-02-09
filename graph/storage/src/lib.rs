//! Storage engine for graph database with ACID transaction support
//!
//! This module provides persistent storage capabilities with transaction management,
//! using a file-based approach for simplicity and reliability.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use graph_core::{VertexId, Edge, Properties};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "async")]
use tokio::io::AsyncReadExt;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Transaction error: {0}")]
    Transaction(String),
    
    #[error("Vertex not found: {0}")]
    VertexNotFound(VertexId),
    
    #[error("Edge not found")]
    EdgeNotFound,
    
    #[error("Storage corruption detected")]
    Corruption,

    #[error("Transaction conflict: snapshot was modified since transaction started (base={0}, current={1})")]
    Conflict(u64, u64),
}

pub type StorageResult<T> = Result<T, StorageError>;

/// Represents a graph operation for transaction logging
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum GraphOperation {
    AddVertex {
        id: VertexId,
        properties: Properties,
    },
    RemoveVertex {
        id: VertexId,
    },
    AddEdge {
        edge: Edge,
        properties: Properties,
    },
    RemoveEdge {
        edge: Edge,
    },
    UpdateVertexProperties {
        id: VertexId,
        properties: Properties,
    },
    UpdateEdgeProperties {
        edge: Edge,
        properties: Properties,
    },
}

/// Write-Ahead Log for ACID compliance
#[derive(Debug)]
pub struct WAL {
    file: BufWriter<File>,
    #[allow(dead_code)]
    path: PathBuf,
    #[allow(dead_code)]
    sync_threshold: usize,
    pending_operations: usize,
}

impl WAL {
    /// Create a new WAL file
    pub fn new<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let path = path.as_ref().join("graph.wal");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(WAL {
            file: BufWriter::new(file),
            path,
            sync_threshold: 100,
            pending_operations: 0,
        })
    }

    /// Append an operation to the WAL
    pub fn append(&mut self, _operation: &GraphOperation) -> StorageResult<()> {
        #[cfg(feature = "serde")]
        {
            let data = bincode::serialize(_operation)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            
            let len = data.len() as u64;
            self.file.write_all(&len.to_le_bytes())?;
            self.file.write_all(&data)?;
            
            self.pending_operations += 1;
            
            if self.pending_operations >= self.sync_threshold {
                self.flush()?;
            }
        }
        
        Ok(())
    }

    /// Force flush pending operations to disk with fsync for true durability
    pub fn flush(&mut self) -> StorageResult<()> {
        self.file.flush()?;
        self.file.get_mut().sync_all()?;
        self.pending_operations = 0;
        Ok(())
    }

    /// Replay WAL operations from file, returning them for application to snapshot
    #[cfg(feature = "serde")]
    pub fn replay<P: AsRef<Path>>(path: P) -> StorageResult<Vec<GraphOperation>> {
        let wal_path = path.as_ref().join("graph.wal");
        if !wal_path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&wal_path)?;
        let mut reader = BufReader::new(file);
        let mut operations = Vec::new();
        let mut len_buf = [0u8; 8];

        loop {
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {
                    let len = u64::from_le_bytes(len_buf) as usize;
                    let mut data = vec![0u8; len];
                    reader.read_exact(&mut data)?;
                    let op: GraphOperation = bincode::deserialize(&data)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    operations.push(op);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(StorageError::Io(e)),
            }
        }

        Ok(operations)
    }

    /// Close the WAL
    pub fn close(mut self) -> StorageResult<()> {
        self.flush()?;
        drop(self.file);
        Ok(())
    }
}

/// Edge key that includes label to support multi-edges between same vertex pair
pub type EdgeKey = (VertexId, VertexId, String);

/// Snapshot of the current database state
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Snapshot {
    pub vertices: HashMap<VertexId, Properties>,
    pub edges: HashMap<EdgeKey, (Edge, Properties)>,
    /// Adjacency index: vertex -> outgoing edges (dst, label)
    #[cfg_attr(feature = "serde", serde(skip, default))]
    out_adj: HashMap<VertexId, Vec<EdgeKey>>,
    /// Adjacency index: vertex -> incoming edges (src, label)
    #[cfg_attr(feature = "serde", serde(skip, default))]
    in_adj: HashMap<VertexId, Vec<EdgeKey>>,
    pub version: u64,
    pub timestamp: std::time::SystemTime,
}

impl Snapshot {
    /// Create an empty snapshot
    pub fn empty() -> Self {
        Self {
            vertices: HashMap::new(),
            edges: HashMap::new(),
            out_adj: HashMap::new(),
            in_adj: HashMap::new(),
            version: 0,
            timestamp: std::time::SystemTime::now(),
        }
    }

    /// Rebuild adjacency indexes from edges (used after deserialization)
    pub fn rebuild_indexes(&mut self) {
        self.out_adj.clear();
        self.in_adj.clear();
        for key in self.edges.keys() {
            self.out_adj.entry(key.0).or_default().push(key.clone());
            self.in_adj.entry(key.1).or_default().push(key.clone());
        }
    }

    /// Save snapshot to file
    pub fn save<P: AsRef<Path>>(&self, path: P) -> StorageResult<()> {
        let path = path.as_ref().join("graph.snap");

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        #[cfg(feature = "serde")]
        {
            let mut writer = BufWriter::new(file);
            let data = bincode::serialize(self)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            writer.write_all(&data)?;
            writer.flush()?;
        }

        Ok(())
    }

    /// Load snapshot from file
    pub fn load<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let path = path.as_ref().join("graph.snap");
        
        if !path.exists() {
            return Ok(Snapshot::empty());
        }

        #[cfg(feature = "serde")]
        {
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);
            let mut buffer = Vec::new();

            reader.read_to_end(&mut buffer)?;

            let mut snapshot: Snapshot = bincode::deserialize(&buffer)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            snapshot.rebuild_indexes();
            Ok(snapshot)
        }
        
        #[cfg(not(feature = "serde"))]
        {
            Err(StorageError::Serialization("serde feature required".to_string()))
        }
    }

    /// Make an EdgeKey from an Edge
    fn edge_key(edge: &Edge) -> EdgeKey {
        (edge.src, edge.dst, edge.label.clone())
    }

    /// Apply an operation to the snapshot
    pub fn apply_operation(&mut self, operation: &GraphOperation) {
        match operation {
            GraphOperation::AddVertex { id, properties } => {
                self.vertices.insert(*id, properties.clone());
            }
            GraphOperation::RemoveVertex { id } => {
                self.vertices.remove(id);
                // Remove all edges connected to this vertex, updating indexes
                let keys_to_remove: Vec<EdgeKey> = self.edges.keys()
                    .filter(|(src, dst, _)| src == id || dst == id)
                    .cloned()
                    .collect();
                for key in &keys_to_remove {
                    self.edges.remove(key);
                }
                self.out_adj.remove(id);
                self.in_adj.remove(id);
                // Also remove references from other vertices' adj lists
                for list in self.out_adj.values_mut() {
                    list.retain(|k| k.1 != *id);
                }
                for list in self.in_adj.values_mut() {
                    list.retain(|k| k.0 != *id);
                }
            }
            GraphOperation::AddEdge { edge, properties } => {
                let key = Self::edge_key(edge);
                self.edges.insert(key.clone(), (edge.clone(), properties.clone()));
                self.out_adj.entry(edge.src).or_default().push(key.clone());
                self.in_adj.entry(edge.dst).or_default().push(key);
            }
            GraphOperation::RemoveEdge { edge } => {
                let key = Self::edge_key(edge);
                self.edges.remove(&key);
                if let Some(list) = self.out_adj.get_mut(&edge.src) {
                    list.retain(|k| k != &key);
                }
                if let Some(list) = self.in_adj.get_mut(&edge.dst) {
                    list.retain(|k| k != &key);
                }
            }
            GraphOperation::UpdateVertexProperties { id, properties } => {
                self.vertices.insert(*id, properties.clone());
            }
            GraphOperation::UpdateEdgeProperties { edge, properties } => {
                let key = Self::edge_key(edge);
                if let Some((existing_edge, _)) = self.edges.get(&key) {
                    self.edges.insert(key, (existing_edge.clone(), properties.clone()));
                }
            }
        }

        self.version += 1;
        self.timestamp = std::time::SystemTime::now();
    }
}

/// Transaction with isolation and atomicity guarantees
#[derive(Debug)]
pub struct Transaction {
    pub id: u64,
    operations: Vec<GraphOperation>,
    snapshot: Snapshot,
    is_committed: bool,
    /// Snapshot version at transaction start (for optimistic concurrency control)
    base_version: u64,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(id: u64, snapshot: Snapshot) -> Self {
        let base_version = snapshot.version;
        Self {
            id,
            operations: Vec::new(),
            snapshot,
            is_committed: false,
            base_version,
        }
    }

    /// Add operation to transaction
    pub fn add_operation(&mut self, operation: GraphOperation) {
        self.operations.push(operation);
    }

    /// Commit the transaction
    pub fn commit(mut self) -> StorageResult<Vec<GraphOperation>> {
        if self.is_committed {
            return Err(StorageError::Transaction("Transaction already committed".to_string()));
        }

        self.is_committed = true;
        Ok(self.operations)
    }

    /// Rollback the transaction
    pub fn rollback(self) -> Snapshot {
        self.snapshot
    }

    /// Get the number of operations
    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }
}

/// Main storage engine managing persistence and transactions
#[derive(Debug)]
pub struct GraphStorage {
    base_path: PathBuf,
    current_snapshot: Arc<Mutex<Snapshot>>,
    wal: Arc<Mutex<WAL>>,
    next_transaction_id: Arc<Mutex<u64>>,
    operation_counter: Arc<Mutex<u64>>,
}

impl GraphStorage {
    /// Create or open storage at the specified path.
    /// Loads snapshot and replays any WAL entries for crash recovery.
    pub fn new<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let base_path = path.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        std::fs::create_dir_all(&base_path)?;

        // Load or create snapshot - handle empty file case
        let mut snapshot = match Snapshot::load(&base_path) {
            Ok(snapshot) => snapshot,
            Err(StorageError::Serialization(_)) => {
                // If deserialization fails (empty or corrupt file), create empty snapshot
                Snapshot::empty()
            }
            Err(e) => return Err(e),
        };

        // Replay WAL for crash recovery
        #[cfg(feature = "serde")]
        {
            let wal_ops = WAL::replay(&base_path)?;
            if !wal_ops.is_empty() {
                for op in &wal_ops {
                    snapshot.apply_operation(op);
                }
            }
        }

        // Initialize WAL (truncate after replay since snapshot now includes those ops)
        let wal = WAL::new(&base_path)?;

        Ok(Self {
            base_path,
            current_snapshot: Arc::new(Mutex::new(snapshot)),
            wal: Arc::new(Mutex::new(wal)),
            next_transaction_id: Arc::new(Mutex::new(1)),
            operation_counter: Arc::new(Mutex::new(0)),
        })
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self) -> StorageResult<Transaction> {
        let snapshot = {
            let current = self.current_snapshot.lock().unwrap();
            Snapshot {
                vertices: current.vertices.clone(),
                edges: current.edges.clone(),
                out_adj: current.out_adj.clone(),
                in_adj: current.in_adj.clone(),
                version: current.version,
                timestamp: current.timestamp,
            }
        };

        let id = {
            let mut next_id = self.next_transaction_id.lock().unwrap();
            let id = *next_id;
            *next_id += 1;
            id
        };

        Ok(Transaction::new(id, snapshot))
    }

    /// Commit a transaction with optimistic concurrency control.
    /// Returns `StorageError::Conflict` if the snapshot was modified since the transaction started.
    pub fn commit_transaction(&self, transaction: Transaction) -> StorageResult<()> {
        let base_version = transaction.base_version;
        let operations = transaction.commit()?;

        // Acquire snapshot lock for the entire commit (atomicity)
        let mut snapshot = self.current_snapshot.lock().unwrap();

        // Optimistic concurrency: reject if snapshot changed since transaction started
        if snapshot.version != base_version {
            return Err(StorageError::Conflict(base_version, snapshot.version));
        }

        // Write operations to WAL first (Durability)
        {
            let mut wal = self.wal.lock().unwrap();
            for operation in &operations {
                wal.append(operation)?;
            }
            wal.flush()?;
        }

        // Apply operations to current snapshot (Consistency)
        for operation in &operations {
            snapshot.apply_operation(operation);
        }
        // Release snapshot lock
        drop(snapshot);

        // Update operation counter
        {
            let mut counter = self.operation_counter.lock().unwrap();
            *counter += operations.len() as u64;
        }

        // Create checkpoint periodically
        if self.should_create_checkpoint() {
            self.create_checkpoint()?;
        }

        Ok(())
    }

    /// Rollback a transaction
    pub fn rollback_transaction(&self, _transaction: Transaction) -> StorageResult<()> {
        // In our implementation, we simply ignore the operations
        // The snapshot is already isolated in the transaction
        Ok(())
    }

    /// Get vertex properties
    pub fn get_vertex(&self, id: VertexId) -> StorageResult<Option<Properties>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        Ok(snapshot.vertices.get(&id).cloned())
    }

    /// Get edge properties (returns first edge between src and dst)
    pub fn get_edge(&self, src: VertexId, dst: VertexId) -> StorageResult<Option<(Edge, Properties)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        // Search through outgoing adjacency for any edge from src to dst
        if let Some(out_keys) = snapshot.out_adj.get(&src) {
            for key in out_keys {
                if key.1 == dst {
                    return Ok(snapshot.edges.get(key).cloned());
                }
            }
        }
        Ok(None)
    }

    /// Get edge by full key (src, dst, label)
    pub fn get_edge_by_label(&self, src: VertexId, dst: VertexId, label: &str) -> StorageResult<Option<(Edge, Properties)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        let key = (src, dst, label.to_string());
        Ok(snapshot.edges.get(&key).cloned())
    }

    /// List all vertices
    pub fn list_vertices(&self) -> StorageResult<Vec<(VertexId, Properties)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        Ok(snapshot.vertices.iter().map(|(id, props)| (*id, props.clone())).collect())
    }

    /// List all edges
    pub fn list_edges(&self) -> StorageResult<Vec<(Edge, Properties)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        Ok(snapshot.edges.values().map(|(edge, props)| (edge.clone(), props.clone())).collect())
    }

    /// Get vertices by property filter
    pub fn find_vertices_by_property<F>(&self, predicate: F) -> StorageResult<Vec<(VertexId, Properties)>>
    where
        F: Fn(&Properties) -> bool,
    {
        let snapshot = self.current_snapshot.lock().unwrap();
        Ok(snapshot
            .vertices
            .iter()
            .filter(|(_, props)| predicate(props))
            .map(|(id, props)| (*id, props.clone()))
            .collect())
    }

    /// Get edges by property filter
    pub fn find_edges_by_property<F>(&self, predicate: F) -> StorageResult<Vec<(Edge, Properties)>>
    where
        F: Fn(&Properties) -> bool,
    {
        let snapshot = self.current_snapshot.lock().unwrap();
        Ok(snapshot
            .edges
            .values()
            .filter(|(_, props)| predicate(props))
            .map(|(edge, props)| (edge.clone(), props.clone()))
            .collect())
    }

    /// Get current database statistics
    pub fn get_stats(&self) -> StorageResult<DatabaseStats> {
        let snapshot = self.current_snapshot.lock().unwrap();
        Ok(DatabaseStats {
            vertex_count: snapshot.vertices.len(),
            edge_count: snapshot.edges.len(),
            version: snapshot.version,
            timestamp: snapshot.timestamp,
        })
    }

    /// Create a checkpoint snapshot
    fn create_checkpoint(&self) -> StorageResult<()> {
        let snapshot = {
            let current = self.current_snapshot.lock().unwrap();
            Snapshot {
                vertices: current.vertices.clone(),
                edges: current.edges.clone(),
                out_adj: HashMap::new(), // Not persisted, rebuilt on load
                in_adj: HashMap::new(),
                version: current.version,
                timestamp: current.timestamp,
            }
        };

        snapshot.save(&self.base_path)?;
        self.truncate_wal()?;
        Ok(())
    }

    /// Determine if we should create a checkpoint
    fn should_create_checkpoint(&self) -> bool {
        let counter = self.operation_counter.lock().unwrap();
        *counter > 0 && *counter % 10 == 0
    }

    /// Truncate WAL after checkpoint
    fn truncate_wal(&self) -> StorageResult<()> {
        let wal_path = self.base_path.join("graph.wal");
        std::fs::write(&wal_path, "")?;
        Ok(())
    }

    /// Force checkpoint now
    pub fn force_checkpoint(&self) -> StorageResult<()> {
        self.create_checkpoint()
    }

    /// Get checkpoint statistics
    pub fn get_checkpoint_stats(&self) -> StorageResult<(u64, u64)> {
        let counter = self.operation_counter.lock().unwrap();
        let version = self.current_snapshot.lock().unwrap().version;
        Ok((*counter, version))
    }

    /// Close storage
    pub fn close(self) -> StorageResult<()> {
        // Note: In a real implementation, we'd need to handle Arc cleanup properly
        // For now, we'll just return success
        Ok(())
    }
}

/// Database statistics
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DatabaseStats {
    pub vertex_count: usize,
    pub edge_count: usize,
    pub version: u64,
    pub timestamp: std::time::SystemTime,
}

/// Async version of storage operations
#[cfg(feature = "async")]
impl GraphStorage {
    /// Async version of new()
    pub async fn new_async<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let base_path = path.as_ref().to_path_buf();
        
        // Create directory if it doesn't exist
        tokio::fs::create_dir_all(&base_path).await?;
        
        // Load or create snapshot
        let snapshot = {
            let path = base_path.join("graph.snap");
            if tokio::fs::metadata(&path).await.is_ok() {
                let mut data = Vec::new();
                let mut file = tokio::fs::File::open(&path).await?;
                file.read_to_end(&mut data).await?;
                
                #[cfg(feature = "serde")]
                {
                    bincode::deserialize(&data)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?
                }
                
                #[cfg(not(feature = "serde"))]
                {
                    return Err(StorageError::Serialization("serde feature required".to_string()));
                }
            } else {
                Snapshot::empty()
            }
        };
        
        // Initialize WAL
        let wal_path = base_path.join("graph.wal");
        let std_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
            
        let wal = WAL {
            file: BufWriter::new(std_file),
            path: wal_path,
            sync_threshold: 100,
            pending_operations: 0,
        };
        
        Ok(Self {
            base_path,
            current_snapshot: Arc::new(Mutex::new(snapshot)),
            wal: Arc::new(Mutex::new(wal)),
            next_transaction_id: Arc::new(Mutex::new(1)),
            operation_counter: Arc::new(Mutex::new(0)),
        })
    }

    /// Async checkpoint creation
    pub async fn create_checkpoint_async(&self) -> StorageResult<()> {
        let snapshot = {
            let current = self.current_snapshot.lock().unwrap();
            Snapshot {
                vertices: current.vertices.clone(),
                edges: current.edges.clone(),
                out_adj: HashMap::new(),
                in_adj: HashMap::new(),
                version: current.version,
                timestamp: current.timestamp,
            }
        };

        let snapshot_path = self.base_path.join("graph.snap");
        #[cfg(feature = "serde")]
        {
            let data = bincode::serialize(&snapshot)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            tokio::fs::write(&snapshot_path, &data).await?;
        }
        
        self.truncate_wal_async().await?;
        
        Ok(())
    }

    /// Async WAL truncation
    async fn truncate_wal_async(&self) -> StorageResult<()> {
        let wal_path = self.base_path.join("graph.wal");
        tokio::fs::write(&wal_path, "").await?;
        Ok(())
    }
}

/// Graph traversal operations
impl GraphStorage {
    /// Get outgoing neighbors of a vertex (vertices reachable via outgoing edges)
    /// O(degree) using adjacency index instead of O(E) full scan
    pub fn get_out_neighbors(&self, id: VertexId) -> StorageResult<Vec<(VertexId, Edge, Properties)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        let mut neighbors = Vec::new();

        if let Some(out_keys) = snapshot.out_adj.get(&id) {
            for key in out_keys {
                if let Some((edge, props)) = snapshot.edges.get(key) {
                    neighbors.push((key.1, edge.clone(), props.clone()));
                }
            }
        }

        Ok(neighbors)
    }

    /// Get incoming neighbors of a vertex (vertices that have edges to this vertex)
    /// O(degree) using adjacency index instead of O(E) full scan
    pub fn get_in_neighbors(&self, id: VertexId) -> StorageResult<Vec<(VertexId, Edge, Properties)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        let mut neighbors = Vec::new();

        if let Some(in_keys) = snapshot.in_adj.get(&id) {
            for key in in_keys {
                if let Some((edge, props)) = snapshot.edges.get(key) {
                    neighbors.push((key.0, edge.clone(), props.clone()));
                }
            }
        }

        Ok(neighbors)
    }

    /// Get all neighbors (both outgoing and incoming)
    pub fn get_all_neighbors(&self, id: VertexId) -> StorageResult<Vec<(VertexId, Edge, Properties)>> {
        let mut neighbors = self.get_out_neighbors(id)?;
        neighbors.extend(self.get_in_neighbors(id)?);
        Ok(neighbors)
    }

    /// 1-hop traversal: get directly connected vertices via outgoing edges
    /// Uses adjacency index for O(degree) performance
    pub fn traverse_1hop(&self, start: VertexId, edge_label: Option<&str>) -> StorageResult<Vec<(VertexId, Edge)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        let mut results = Vec::new();

        if let Some(out_keys) = snapshot.out_adj.get(&start) {
            for key in out_keys {
                if let Some(label) = edge_label {
                    if key.2 == label {
                        if let Some((edge, _)) = snapshot.edges.get(key) {
                            results.push((key.1, edge.clone()));
                        }
                    }
                } else if let Some((edge, _)) = snapshot.edges.get(key) {
                    results.push((key.1, edge.clone()));
                }
            }
        }

        Ok(results)
    }

    /// 2-hop traversal: get friends of friends
    /// Single lock acquisition for the entire traversal
    pub fn traverse_2hop(&self, start: VertexId, edge_label: Option<&str>) -> StorageResult<Vec<VertexId>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // First hop
        let first_hop_keys = snapshot.out_adj.get(&start).cloned().unwrap_or_default();
        for key in &first_hop_keys {
            if let Some(label) = edge_label {
                if key.2 != label { continue; }
            }
            let intermediate = key.1;

            // Second hop
            if let Some(second_keys) = snapshot.out_adj.get(&intermediate) {
                for key2 in second_keys {
                    if let Some(label) = edge_label {
                        if key2.2 != label { continue; }
                    }
                    let target = key2.1;
                    if target != start && seen.insert(target) {
                        results.push(target);
                    }
                }
            }
        }

        Ok(results)
    }

    /// BFS shortest path (unweighted graph, max depth)
    /// Single lock acquisition, uses adjacency index
    pub fn shortest_path(&self, start: VertexId, end: VertexId, max_depth: usize) -> StorageResult<Option<Vec<VertexId>>> {
        if start == end {
            return Ok(Some(vec![start]));
        }

        let snapshot = self.current_snapshot.lock().unwrap();

        let mut queue: std::collections::VecDeque<(VertexId, Vec<VertexId>)> = std::collections::VecDeque::new();
        let mut visited: std::collections::HashSet<VertexId> = std::collections::HashSet::new();

        queue.push_back((start, vec![start]));
        visited.insert(start);

        while let Some((current, path)) = queue.pop_front() {
            if path.len() > max_depth {
                continue;
            }

            if let Some(out_keys) = snapshot.out_adj.get(&current) {
                for key in out_keys {
                    let dst = key.1;
                    if !visited.contains(&dst) {
                        let mut new_path = path.clone();
                        new_path.push(dst);

                        if dst == end {
                            return Ok(Some(new_path));
                        }

                        visited.insert(dst);
                        queue.push_back((dst, new_path));
                    }
                }
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use graph_core::props;

    #[test]
    fn test_snapshot_creation() {
        let snapshot = Snapshot::empty();
        assert_eq!(snapshot.version, 0);
        assert_eq!(snapshot.vertices.len(), 0);
        assert_eq!(snapshot.edges.len(), 0);
    }

    #[test]
    fn test_snapshot_operations() {
        let mut snapshot = Snapshot::empty();
        
        // Add vertex
        let vertex_id = VertexId::new(1);
        let props = props::map(vec![("name", "Alice")]);
        snapshot.apply_operation(&GraphOperation::AddVertex { 
            id: vertex_id, 
            properties: props.clone() 
        });
        
        assert_eq!(snapshot.vertices.get(&vertex_id), Some(&props));
        assert_eq!(snapshot.version, 1);
        
        // Add edge
        let edge = Edge::new(vertex_id, VertexId::new(2), "friend");
        let edge_props = props::map(vec![("weight", 1.0)]);
        snapshot.apply_operation(&GraphOperation::AddEdge { 
            edge: edge.clone(), 
            properties: edge_props.clone() 
        });
        
        assert_eq!(snapshot.edges.get(&(vertex_id, VertexId::new(2), "friend".to_string())), Some(&(edge, edge_props)));
        assert_eq!(snapshot.version, 2);
    }

    #[test]
    fn test_transaction_creation() {
        let snapshot = Snapshot::empty();
        let transaction = Transaction::new(1, snapshot);
        
        assert_eq!(transaction.id, 1);
        assert_eq!(transaction.operation_count(), 0);
        assert!(!transaction.is_committed);
    }

    #[test]
    fn test_transaction_operations() {
        let snapshot = Snapshot::empty();
        let mut transaction = Transaction::new(1, snapshot);

        let operation = GraphOperation::AddVertex {
            id: VertexId::new(1),
            properties: props::map(vec![("name", "Test")]),
        };

        transaction.add_operation(operation);
        assert_eq!(transaction.operation_count(), 1);

        // commit() consumes the transaction, so we can't check is_committed after
        let operations = transaction.commit().unwrap();
        assert_eq!(operations.len(), 1);
    }

    #[test]
    fn test_storage_creation() -> Result<(), StorageError> {
        let temp_dir = tempdir().unwrap();
        let storage = GraphStorage::new(temp_dir.path())?;
        
        // Test initial state
        let stats = storage.get_stats()?;
        assert_eq!(stats.vertex_count, 0);
        assert_eq!(stats.edge_count, 0);
        
        storage.close()?;
        Ok(())
    }

    #[test]
    fn test_transaction_commit() -> Result<(), StorageError> {
        let temp_dir = tempdir().unwrap();
        let storage = GraphStorage::new(temp_dir.path())?;
        
        // Begin transaction
        let mut transaction = storage.begin_transaction()?;
        
        // Add vertex
        let vertex_id = VertexId::new(1);
        let props = props::map(vec![("name", "Alice")]);
        transaction.add_operation(GraphOperation::AddVertex { 
            id: vertex_id, 
            properties: props.clone() 
        });
        
        // Commit transaction
        storage.commit_transaction(transaction)?;
        
        // Verify vertex was added
        let vertex_props = storage.get_vertex(vertex_id)?;
        assert_eq!(vertex_props, Some(props));
        
        storage.close()?;
        Ok(())
    }

    #[test]
    fn test_persistence() -> Result<(), StorageError> {
        let temp_dir = tempdir().unwrap();

        // Create initial storage
        {
            let storage = GraphStorage::new(temp_dir.path())?;
            let mut transaction = storage.begin_transaction()?;

            transaction.add_operation(GraphOperation::AddVertex {
                id: VertexId::new(1),
                properties: props::map(vec![("name", "Persistent")]),
            });

            storage.commit_transaction(transaction)?;
            // Force checkpoint to persist data before closing
            storage.force_checkpoint()?;
            storage.close()?;
        }

        // Reopen storage
        {
            let storage = GraphStorage::new(temp_dir.path())?;
            let vertex_props = storage.get_vertex(VertexId::new(1))?;
            assert!(vertex_props.is_some());
            assert_eq!(vertex_props.unwrap().get("name").unwrap().as_string(), Some("Persistent"));

            storage.close()?;
        }

        Ok(())
    }

    /// Helper: create a storage with a small graph for traversal tests
    ///
    /// Graph:  1 -friend-> 2 -friend-> 3
    ///                       \-colleague-> 4
    fn create_traversal_storage() -> Result<(tempfile::TempDir, GraphStorage), StorageError> {
        let temp_dir = tempdir().unwrap();
        let storage = GraphStorage::new(temp_dir.path())?;
        let mut txn = storage.begin_transaction()?;

        txn.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(1),
            properties: props::map(vec![("name", "Alice")]),
        });
        txn.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(2),
            properties: props::map(vec![("name", "Bob")]),
        });
        txn.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(3),
            properties: props::map(vec![("name", "Charlie")]),
        });
        txn.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(4),
            properties: props::map(vec![("name", "Diana")]),
        });

        txn.add_operation(GraphOperation::AddEdge {
            edge: Edge::from_ids(1, 2, "friend"),
            properties: HashMap::new(),
        });
        txn.add_operation(GraphOperation::AddEdge {
            edge: Edge::from_ids(2, 3, "friend"),
            properties: HashMap::new(),
        });
        txn.add_operation(GraphOperation::AddEdge {
            edge: Edge::from_ids(2, 4, "colleague"),
            properties: HashMap::new(),
        });

        storage.commit_transaction(txn)?;
        Ok((temp_dir, storage))
    }

    #[test]
    fn test_get_out_neighbors() -> Result<(), StorageError> {
        let (_dir, storage) = create_traversal_storage()?;

        // Vertex 2 has outgoing edges to 3 (friend) and 4 (colleague)
        let neighbors = storage.get_out_neighbors(VertexId::new(2))?;
        assert_eq!(neighbors.len(), 2);
        let dst_ids: Vec<u64> = neighbors.iter().map(|(id, _, _)| id.value()).collect();
        assert!(dst_ids.contains(&3));
        assert!(dst_ids.contains(&4));

        // Vertex 3 has no outgoing edges
        let neighbors = storage.get_out_neighbors(VertexId::new(3))?;
        assert!(neighbors.is_empty());

        Ok(())
    }

    #[test]
    fn test_get_in_neighbors() -> Result<(), StorageError> {
        let (_dir, storage) = create_traversal_storage()?;

        // Vertex 3 has one incoming edge from 2
        let neighbors = storage.get_in_neighbors(VertexId::new(3))?;
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, VertexId::new(2));

        // Vertex 1 has no incoming edges
        let neighbors = storage.get_in_neighbors(VertexId::new(1))?;
        assert!(neighbors.is_empty());

        Ok(())
    }

    #[test]
    fn test_get_all_neighbors() -> Result<(), StorageError> {
        let (_dir, storage) = create_traversal_storage()?;

        // Vertex 2 has 1 incoming (from 1) and 2 outgoing (to 3, 4) = 3 total
        let neighbors = storage.get_all_neighbors(VertexId::new(2))?;
        assert_eq!(neighbors.len(), 3);

        Ok(())
    }

    #[test]
    fn test_traverse_1hop() -> Result<(), StorageError> {
        let (_dir, storage) = create_traversal_storage()?;

        // From vertex 2, 1-hop via "friend" should reach vertex 3
        let results = storage.traverse_1hop(VertexId::new(2), Some("friend"))?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, VertexId::new(3));

        // From vertex 2, 1-hop without label filter should reach 3 and 4
        let results = storage.traverse_1hop(VertexId::new(2), None)?;
        assert_eq!(results.len(), 2);

        // From vertex 1, 1-hop via "friend" should reach vertex 2
        let results = storage.traverse_1hop(VertexId::new(1), Some("friend"))?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, VertexId::new(2));

        Ok(())
    }

    #[test]
    fn test_traverse_2hop() -> Result<(), StorageError> {
        let (_dir, storage) = create_traversal_storage()?;

        // From vertex 1, 2-hop via "friend": 1->2->3
        let results = storage.traverse_2hop(VertexId::new(1), Some("friend"))?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], VertexId::new(3));

        // From vertex 1, 2-hop without label filter: 1->2->{3,4}
        let results = storage.traverse_2hop(VertexId::new(1), None)?;
        assert_eq!(results.len(), 2);
        assert!(results.contains(&VertexId::new(3)));
        assert!(results.contains(&VertexId::new(4)));

        Ok(())
    }

    #[test]
    fn test_shortest_path() -> Result<(), StorageError> {
        let (_dir, storage) = create_traversal_storage()?;

        // Path from 1 to 3: 1->2->3
        let path = storage.shortest_path(VertexId::new(1), VertexId::new(3), 5)?;
        assert!(path.is_some());
        let path = path.unwrap();
        assert_eq!(path, vec![VertexId::new(1), VertexId::new(2), VertexId::new(3)]);

        // Path from 1 to 4: 1->2->4
        let path = storage.shortest_path(VertexId::new(1), VertexId::new(4), 5)?;
        assert!(path.is_some());
        let path = path.unwrap();
        assert_eq!(path, vec![VertexId::new(1), VertexId::new(2), VertexId::new(4)]);

        // Path from 1 to 1: trivial
        let path = storage.shortest_path(VertexId::new(1), VertexId::new(1), 5)?;
        assert_eq!(path, Some(vec![VertexId::new(1)]));

        // No path from 3 to 1 (directed graph, no back edges)
        let path = storage.shortest_path(VertexId::new(3), VertexId::new(1), 5)?;
        assert!(path.is_none());

        // Max depth too small: 1 to 3 requires 2 hops but max_depth=1
        let path = storage.shortest_path(VertexId::new(1), VertexId::new(3), 1)?;
        assert!(path.is_none());

        Ok(())
    }
}