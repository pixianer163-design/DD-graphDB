//! Basic graph algorithms
//!
//! This module provides fundamental graph algorithms that work directly
//! with GraphStorage without requiring streaming features.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use graph_core::{VertexId, Edge, Properties, PropertyValue};
use graph_storage::GraphStorage;

/// PageRank algorithm result
#[derive(Debug, Clone)]
pub struct PageRankResult {
    /// Vertex ID to PageRank score mapping
    pub scores: HashMap<VertexId, f64>,
    /// Number of iterations performed
    pub iterations: usize,
    /// Convergence delta (max change in last iteration)
    pub convergence_delta: f64,
}

impl PageRankResult {
    /// Get top N vertices by PageRank score
    pub fn top_n(&self, n: usize) -> Vec<(VertexId, f64)> {
        let mut items: Vec<(VertexId, f64)> = self.scores.iter()
            .map(|(k, v)| (*k, *v))
            .collect();
        items.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        items.into_iter().take(n).collect()
    }
    
    /// Get score for a specific vertex
    pub fn get_score(&self, vertex: VertexId) -> Option<f64> {
        self.scores.get(&vertex).copied()
    }
}

/// Compute PageRank using iterative power method
/// 
/// # Arguments
/// * `storage` - Graph storage
/// * `damping_factor` - Damping factor (typically 0.85)
/// * `iterations` - Maximum number of iterations
/// * `tolerance` - Convergence tolerance
pub fn compute_pagerank(
    storage: &GraphStorage,
    damping_factor: f64,
    iterations: usize,
    tolerance: f64,
) -> Result<PageRankResult, Box<dyn std::error::Error>> {
    // Get all vertices and edges
    let vertices: Vec<(VertexId, Properties)> = storage.list_vertices()?;
    let edges: Vec<(Edge, Properties)> = storage.list_edges()?;
    
    let n = vertices.len();
    if n == 0 {
        return Ok(PageRankResult {
            scores: HashMap::new(),
            iterations: 0,
            convergence_delta: 0.0,
        });
    }
    
    // Build adjacency list (outgoing edges)
    let mut outgoing: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
    let mut incoming: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
    
    for (edge, _) in &edges {
        outgoing.entry(edge.src).or_default().push(edge.dst);
        incoming.entry(edge.dst).or_default().push(edge.src);
    }
    
    // Initialize PageRank scores
    let initial_score = 1.0 / n as f64;
    let mut scores: HashMap<VertexId, f64> = vertices.iter()
        .map(|(id, _)| (*id, initial_score))
        .collect();
    
    let mut last_delta = tolerance;
    
    // Power iteration
    for iteration in 0..iterations {
        let mut new_scores: HashMap<VertexId, f64> = HashMap::new();
        let mut max_delta = 0.0;
        
        for (vertex_id, _) in &vertices {
            // Calculate incoming contribution
            let mut incoming_sum = 0.0;
            
            if let Some(in_neighbors) = incoming.get(vertex_id) {
                for neighbor_id in in_neighbors {
                    if let Some(neighbor_score) = scores.get(neighbor_id) {
                        let out_degree = outgoing.get(neighbor_id).map(|v| v.len()).unwrap_or(1);
                        incoming_sum += neighbor_score / out_degree as f64;
                    }
                }
            }
            
            // Apply damping factor
            let new_score = (1.0 - damping_factor) / n as f64 + damping_factor * incoming_sum;
            new_scores.insert(*vertex_id, new_score);
            
            // Calculate delta
            if let Some(old_score) = scores.get(vertex_id) {
                let delta = (new_score - old_score).abs();
                if delta > max_delta {
                    max_delta = delta;
                }
            }
        }
        
        scores = new_scores;
        last_delta = max_delta;
        
        // Check convergence
        if max_delta < tolerance {
            return Ok(PageRankResult {
                scores,
                iterations: iteration + 1,
                convergence_delta: max_delta,
            });
        }
    }
    
    Ok(PageRankResult {
        scores,
        iterations,
        convergence_delta: last_delta,
    })
}

/// Connected components using Union-Find
#[derive(Debug, Clone)]
pub struct ConnectedComponentsResult {
    /// Vertex ID to component ID mapping
    pub components: HashMap<VertexId, VertexId>,
    /// Number of connected components
    pub num_components: usize,
}

impl ConnectedComponentsResult {
    /// Get all vertices in a specific component
    pub fn get_component_vertices(&self, component_id: VertexId) -> Vec<VertexId> {
        self.components.iter()
            .filter(|(_, &comp)| comp == component_id)
            .map(|(v, _)| *v)
            .collect()
    }
    
    /// Get component ID for a vertex
    pub fn get_component(&self, vertex: VertexId) -> Option<VertexId> {
        self.components.get(&vertex).copied()
    }
}

/// Find connected components using Union-Find algorithm
pub fn find_connected_components(
    storage: &GraphStorage,
) -> Result<ConnectedComponentsResult, Box<dyn std::error::Error>> {
    let vertices: Vec<(VertexId, Properties)> = storage.list_vertices()?;
    let edges: Vec<(Edge, Properties)> = storage.list_edges()?;
    
    // Initialize Union-Find: each vertex is its own parent
    let mut parent: HashMap<VertexId, VertexId> = vertices.iter()
        .map(|(id, _)| (*id, *id))
        .collect();
    
    fn find(parent: &mut HashMap<VertexId, VertexId>, x: VertexId) -> VertexId {
        let p = *parent.get(&x).unwrap();
        if p != x {
            let root = find(parent, p);
            parent.insert(x, root);
            return root;
        }
        x
    }
    
    fn union(parent: &mut HashMap<VertexId, VertexId>, x: VertexId, y: VertexId) {
        let root_x = find(parent, x);
        let root_y = find(parent, y);
        if root_x != root_y {
            parent.insert(root_x, root_y);
        }
    }
    
    // Union all connected vertices
    for (edge, _) in &edges {
        union(&mut parent, edge.src, edge.dst);
    }
    
    // Find final component for each vertex
    let mut components: HashMap<VertexId, VertexId> = HashMap::new();
    let mut unique_components: HashSet<VertexId> = HashSet::new();
    
    for (vertex_id, _) in &vertices {
        let root = find(&mut parent, *vertex_id);
        components.insert(*vertex_id, root);
        unique_components.insert(root);
    }
    
    Ok(ConnectedComponentsResult {
        components,
        num_components: unique_components.len(),
    })
}

/// Dijkstra shortest path result
#[derive(Debug, Clone)]
pub struct DijkstraResult {
    /// Distance from start to each vertex
    pub distances: HashMap<VertexId, f64>,
    /// Previous vertex in shortest path
    pub previous: HashMap<VertexId, VertexId>,
    /// The path from start to end (if end was specified)
    pub path: Option<Vec<VertexId>>,
}

impl DijkstraResult {
    /// Get distance to a specific vertex
    pub fn get_distance(&self, vertex: VertexId) -> Option<f64> {
        self.distances.get(&vertex).copied()
    }
    
    /// Reconstruct path from start to a vertex
    pub fn get_path_to(&self, vertex: VertexId) -> Option<Vec<VertexId>> {
        if !self.distances.contains_key(&vertex) {
            return None;
        }
        
        let mut path = vec![vertex];
        let mut current = vertex;
        
        while let Some(&prev) = self.previous.get(&current) {
            path.push(prev);
            current = prev;
        }
        
        path.reverse();
        Some(path)
    }
}

/// Dijkstra shortest path algorithm
/// 
/// # Arguments
/// * `storage` - Graph storage
/// * `start` - Starting vertex
/// * `end` - Optional ending vertex (if None, computes all distances from start)
/// * `weight_property` - Property name to use as edge weight (if None, uses 1.0)
pub fn compute_shortest_path(
    storage: &GraphStorage,
    start: VertexId,
    end: Option<VertexId>,
    weight_property: Option<&str>,
) -> Result<DijkstraResult, Box<dyn std::error::Error>> {
    let mut distances: HashMap<VertexId, f64> = HashMap::new();
    let mut previous: HashMap<VertexId, VertexId> = HashMap::new();
    let mut visited: HashSet<VertexId> = HashSet::new();
    
    // Priority queue: (distance, vertex_id)
    let mut queue: VecDeque<(f64, VertexId)> = VecDeque::new();
    
    distances.insert(start, 0.0);
    queue.push_back((0.0, start));
    
    while let Some((current_dist, current)) = queue.pop_front() {
        if visited.contains(&current) {
            continue;
        }
        
        visited.insert(current);
        
        // Check if we reached the target
        if let Some(target) = end {
            if current == target {
                let path = if visited.contains(&target) {
                    reconstruct_path(&previous, start, target)
                } else {
                    None
                };
                
                return Ok(DijkstraResult {
                    distances,
                    previous,
                    path,
                });
            }
        }
        
        // Get outgoing neighbors
        let neighbors = storage.get_out_neighbors(current)?;
        
        for (neighbor_id, edge, edge_props) in neighbors {
            if visited.contains(&neighbor_id) {
                continue;
            }
            
            // Get edge weight
            let weight = if let Some(prop_name) = weight_property {
                edge_props.get(prop_name)
                    .and_then(|v| match v {
                        PropertyValue::Float64(f) => Some(*f),
                        PropertyValue::Int64(i) => Some(*i as f64),
                        _ => None,
                    })
                    .unwrap_or(1.0)
            } else {
                1.0
            };
            
            let new_dist = current_dist + weight;
            
            if !distances.contains_key(&neighbor_id) || new_dist < distances[&neighbor_id] {
                distances.insert(neighbor_id, new_dist);
                previous.insert(neighbor_id, current);
                queue.push_back((new_dist, neighbor_id));
            }
        }
        
        // Sort queue by distance (simple implementation)
        let mut queue_vec: Vec<(f64, VertexId)> = queue.drain(..).collect();
        queue_vec.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        queue = queue_vec.into_iter().collect();
    }
    
    // If we had a target but didn't reach it
    if let Some(target) = end {
        if !distances.contains_key(&target) {
            return Ok(DijkstraResult {
                distances,
                previous,
                path: None,
            });
        }
    }
    
    // Compute path before moving previous into result
    let path = end.and_then(|target| reconstruct_path(&previous, start, target));
    
    Ok(DijkstraResult {
        distances,
        previous,
        path,
    })
}

/// Reconstruct path from previous mapping
fn reconstruct_path(
    previous: &HashMap<VertexId, VertexId>,
    start: VertexId,
    end: VertexId,
) -> Option<Vec<VertexId>> {
    let mut path = vec![end];
    let mut current = end;
    
    while current != start {
        if let Some(&prev) = previous.get(&current) {
            path.push(prev);
            current = prev;
        } else {
            return None;
        }
    }
    
    path.reverse();
    Some(path)
}

/// Algorithm statistics
#[derive(Debug, Clone)]
pub struct AlgorithmStats {
    pub algorithm_name: String,
    pub execution_time_ms: u128,
    pub vertices_processed: usize,
    pub edges_processed: usize,
}

/// Run algorithm with timing
pub fn run_with_timing<T>(
    name: &str,
    f: impl FnOnce() -> Result<T, Box<dyn std::error::Error>>,
) -> Result<(T, AlgorithmStats), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let result = f()?;
    let elapsed = start.elapsed();
    
    Ok((result, AlgorithmStats {
        algorithm_name: name.to_string(),
        execution_time_ms: elapsed.as_millis(),
        vertices_processed: 0, // Can be updated by caller
        edges_processed: 0,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_storage::GraphOperation;
    use graph_core::props;

    fn create_test_graph() -> Arc<GraphStorage> {
        let temp_dir = std::env::temp_dir().join("algo_test");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        let storage = Arc::new(GraphStorage::new(&temp_dir).unwrap());
        let mut transaction = storage.begin_transaction().unwrap();
        
        // Create a simple graph: A -> B -> C, A -> C
        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(1),
            properties: props::map(vec![("name", "A")]),
        });
        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(2),
            properties: props::map(vec![("name", "B")]),
        });
        transaction.add_operation(GraphOperation::AddVertex {
            id: VertexId::new(3),
            properties: props::map(vec![("name", "C")]),
        });
        
        // A -> B
        transaction.add_operation(GraphOperation::AddEdge {
            edge: Edge::new(VertexId::new(1), VertexId::new(2), "link"),
            properties: HashMap::new(),
        });
        // B -> C
        transaction.add_operation(GraphOperation::AddEdge {
            edge: Edge::new(VertexId::new(2), VertexId::new(3), "link"),
            properties: HashMap::new(),
        });
        // A -> C
        transaction.add_operation(GraphOperation::AddEdge {
            edge: Edge::new(VertexId::new(1), VertexId::new(3), "link"),
            properties: HashMap::new(),
        });
        
        storage.commit_transaction(transaction).unwrap();
        storage
    }

    #[test]
    fn test_pagerank() {
        let storage = create_test_graph();
        let result = pagerank(&storage, 0.85, 100, 0.0001).unwrap();
        
        assert!(!result.scores.is_empty());
        assert!(result.iterations > 0);
        assert!(result.iterations <= 100);
        
        // All scores should sum to approximately 1.0
        let total: f64 = result.scores.values().sum();
        assert!((total - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_connected_components() {
        let storage = create_test_graph();
        let result = connected_components(&storage).unwrap();
        
        // All vertices should be in the same component
        assert_eq!(result.num_components, 1);
    }

    #[test]
    fn test_dijkstra() {
        let storage = create_test_graph();
        let result = dijkstra(&storage, VertexId::new(1), Some(VertexId::new(3)), None).unwrap();
        
        assert!(result.distances.contains_key(&VertexId::new(3)));
        assert_eq!(result.distances[&VertexId::new(3)], 1.0); // Direct edge A -> C
        assert!(result.path.is_some());
        assert_eq!(result.path.unwrap().len(), 2); // [A, C]
    }
}
