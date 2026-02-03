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

    /// Force flush pending operations to disk
    pub fn flush(&mut self) -> StorageResult<()> {
        self.file.flush()?;
        self.pending_operations = 0;
        Ok(())
    }

    /// Close the WAL
    pub fn close(mut self) -> StorageResult<()> {
        self.flush()?;
        drop(self.file);
        Ok(())
    }
}

/// Snapshot of the current database state
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Snapshot {
    pub vertices: HashMap<VertexId, Properties>,
    pub edges: HashMap<(VertexId, VertexId), (Edge, Properties)>,
    pub version: u64,
    pub timestamp: std::time::SystemTime,
}

impl Snapshot {
    /// Create an empty snapshot
    pub fn empty() -> Self {
        Self {
            vertices: HashMap::new(),
            edges: HashMap::new(),
            version: 0,
            timestamp: std::time::SystemTime::now(),
        }
    }

    /// Save snapshot to file
    pub fn save<P: AsRef<Path>>(&self, path: P) -> StorageResult<()> {
        let path = path.as_ref().join("graph.snap");
        eprintln!("💾 Saving snapshot to: {:?}", path);
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| {
                eprintln!("❌ Failed to open snapshot file: {}", e);
                e
            })?;

        #[cfg(feature = "serde")]
        {
            let mut writer = BufWriter::new(file);
            let data = bincode::serialize(self)
                .map_err(|e| {
                    eprintln!("❌ Failed to serialize snapshot: {}", e);
                    StorageError::Serialization(e.to_string())
                })?;
            
            writer.write_all(&data)
                .map_err(|e| {
                    eprintln!("❌ Failed to write snapshot data: {}", e);
                    e
                })?;
                
            writer.flush()
                .map_err(|e| {
                    eprintln!("❌ Failed to flush snapshot: {}", e);
                    e
                })?;
            
            eprintln!("✅ Snapshot saved successfully, {} bytes", data.len());
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
            
            let snapshot: Snapshot = bincode::deserialize(&buffer)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            Ok(snapshot)
        }
        
        #[cfg(not(feature = "serde"))]
        {
            Err(StorageError::Serialization("serde feature required".to_string()))
        }
    }

    /// Apply an operation to the snapshot
    pub fn apply_operation(&mut self, operation: &GraphOperation) {
        match operation {
            GraphOperation::AddVertex { id, properties } => {
                self.vertices.insert(*id, properties.clone());
            }
            GraphOperation::RemoveVertex { id } => {
                self.vertices.remove(id);
                // Remove all edges connected to this vertex
                self.edges.retain(|(src, dst), _| src != id && dst != id);
            }
            GraphOperation::AddEdge { edge, properties } => {
                self.edges.insert((edge.src, edge.dst), (edge.clone(), properties.clone()));
            }
            GraphOperation::RemoveEdge { edge } => {
                self.edges.remove(&(edge.src, edge.dst));
            }
            GraphOperation::UpdateVertexProperties { id, properties } => {
                self.vertices.insert(*id, properties.clone());
            }
            GraphOperation::UpdateEdgeProperties { edge, properties } => {
                if let Some((existing_edge, _)) = self.edges.get(&(edge.src, edge.dst)) {
                    self.edges.insert((edge.src, edge.dst), (existing_edge.clone(), properties.clone()));
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
}

impl Transaction {
    /// Create a new transaction
    pub fn new(id: u64, snapshot: Snapshot) -> Self {
        Self {
            id,
            operations: Vec::new(),
            snapshot,
            is_committed: false,
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
    /// Create or open storage at the specified path
    pub fn new<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let base_path = path.as_ref().to_path_buf();
        
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&base_path)?;
        
        // Load or create snapshot - handle empty file case
        let snapshot = match Snapshot::load(&base_path) {
            Ok(snapshot) => snapshot,
            Err(StorageError::Serialization(_)) => {
                // If deserialization fails (empty or corrupt file), create empty snapshot
                Snapshot::empty()
            }
            Err(e) => return Err(e),
        };
        
        // Initialize WAL
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

    /// Commit a transaction
    pub fn commit_transaction(&self, transaction: Transaction) -> StorageResult<()> {
        let operations = transaction.commit()?;
        
        // Write operations to WAL first (Durability)
        {
            let mut wal = self.wal.lock().unwrap();
            for operation in &operations {
                wal.append(operation)?;
            }
            wal.flush()?;
        }

        // Apply operations to current snapshot (Consistency)
        {
            let mut snapshot = self.current_snapshot.lock().unwrap();
            for operation in &operations {
                snapshot.apply_operation(operation);
            }
        }

        // Update operation counter
        {
            let mut counter = self.operation_counter.lock().unwrap();
            *counter += operations.len() as u64;
            eprintln!("📊 Operation counter updated: {} (+{} operations)", *counter, operations.len());
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

    /// Get edge properties
    pub fn get_edge(&self, src: VertexId, dst: VertexId) -> StorageResult<Option<(Edge, Properties)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        Ok(snapshot.edges.get(&(src, dst)).cloned())
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
        eprintln!("💾 Creating checkpoint snapshot...");
        
        let snapshot = {
            let current = self.current_snapshot.lock().unwrap();
            eprintln!("📋 Current snapshot version: {}, vertices: {}, edges: {}", 
                     current.version, current.vertices.len(), current.edges.len());
            Snapshot {
                vertices: current.vertices.clone(),
                edges: current.edges.clone(),
                version: current.version,
                timestamp: current.timestamp,
            }
        };

        snapshot.save(&self.base_path)?;
        eprintln!("✅ Checkpoint saved successfully");
        
        // Truncate WAL after successful checkpoint
        self.truncate_wal()?;
        eprintln!("🗑️  WAL truncated after checkpoint");
        
        Ok(())
    }

    /// Determine if we should create a checkpoint
    fn should_create_checkpoint(&self) -> bool {
        // Simple heuristic: create checkpoint every 10 operations for testing
        let counter = self.operation_counter.lock().unwrap();
        let should_create = *counter > 0 && *counter % 10 == 0;
        if should_create {
            eprintln!("📝 Checkpoint triggered at operation count: {}", *counter);
        }
        should_create
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
    pub fn get_out_neighbors(&self, id: VertexId) -> StorageResult<Vec<(VertexId, Edge, Properties)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        let mut neighbors = Vec::new();
        
        for ((src, dst), (edge, props)) in snapshot.edges.iter() {
            if *src == id {
                neighbors.push((*dst, edge.clone(), props.clone()));
            }
        }
        
        Ok(neighbors)
    }
    
    /// Get incoming neighbors of a vertex (vertices that have edges to this vertex)
    pub fn get_in_neighbors(&self, id: VertexId) -> StorageResult<Vec<(VertexId, Edge, Properties)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        let mut neighbors = Vec::new();
        
        for ((src, dst), (edge, props)) in snapshot.edges.iter() {
            if *dst == id {
                neighbors.push((*src, edge.clone(), props.clone()));
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
    
    /// 1-hop traversal: get directly connected vertices
    pub fn traverse_1hop(&self, start: VertexId, edge_label: Option<&str>) -> StorageResult<Vec<(VertexId, Edge)>> {
        let snapshot = self.current_snapshot.lock().unwrap();
        let mut results = Vec::new();
        
        for ((src, dst), (edge, _props)) in snapshot.edges.iter() {
            if *src == start {
                // Check edge label filter if provided
                if let Some(label) = edge_label {
                    if edge.label == label {
                        results.push((*dst, edge.clone()));
                    }
                } else {
                    results.push((*dst, edge.clone()));
                }
            }
        }
        
        Ok(results)
    }
    
    /// 2-hop traversal: get friends of friends
    pub fn traverse_2hop(&self, start: VertexId, edge_label: Option<&str>) -> StorageResult<Vec<VertexId>> {
        let first_hop = self.traverse_1hop(start, edge_label)?;
        let mut second_hop_results = Vec::new();
        let mut seen = std::collections::HashSet::new();
        
        for (intermediate_id, _edge) in first_hop {
            let second_hop = self.traverse_1hop(intermediate_id, edge_label)?;
            for (target_id, _edge) in second_hop {
                // Avoid cycles and don't include the start vertex
                if target_id != start && !seen.contains(&target_id) {
                    seen.insert(target_id);
                    second_hop_results.push(target_id);
                }
            }
        }
        
        Ok(second_hop_results)
    }
    
    /// BFS shortest path (unweighted graph, max depth 5)
    pub fn shortest_path(&self, start: VertexId, end: VertexId, max_depth: usize) -> StorageResult<Option<Vec<VertexId>>> {
        if start == end {
            return Ok(Some(vec![start]));
        }
        
        let snapshot = self.current_snapshot.lock().unwrap();
        
        // BFS setup
        let mut queue: std::collections::VecDeque<(VertexId, Vec<VertexId>)> = std::collections::VecDeque::new();
        let mut visited: std::collections::HashSet<VertexId> = std::collections::HashSet::new();
        
        queue.push_back((start, vec![start]));
        visited.insert(start);
        
        while let Some((current, path)) = queue.pop_front() {
            if path.len() > max_depth {
                continue;
            }
            
            // Get outgoing edges
            for ((src, dst), _) in snapshot.edges.iter() {
                if *src == current && !visited.contains(dst) {
                    let mut new_path = path.clone();
                    new_path.push(*dst);
                    
                    if *dst == end {
                        return Ok(Some(new_path));
                    }
                    
                    visited.insert(*dst);
                    queue.push_back((*dst, new_path));
                }
            }
        }
        
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
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
        
        assert_eq!(snapshot.edges.get(&(vertex_id, VertexId::new(2))), Some(&(edge, edge_props)));
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
        
        let operations = transaction.commit().unwrap();
        assert_eq!(operations.len(), 1);
        assert!(transaction.is_committed);
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
}