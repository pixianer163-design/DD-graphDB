//! Graph algorithms using differential dataflow for incremental computation
//!
//! This module provides efficient implementations of common graph algorithms
//! that leverage differential dataflow's incremental update capabilities.

#[cfg(feature = "streaming")]
use differential_dataflow::collection::Collection;
#[cfg(feature = "streaming")]
use timely::dataflow::Scope;

use graph_core::{VertexId, Edge};

/// Compute reachability from all vertices using fixed-point iteration
#[cfg(feature = "streaming")]
pub fn reachability<G: Scope>(
    edges: &Collection<G, Edge>,
) -> Collection<G, (VertexId, VertexId)>
where
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    // Start with all vertices as sources
    let vertices = edges
        .map(|edge| edge.src)
        .concat(&edges.map(|edge| edge.dst))
        .distinct();

    // Fixed-point iteration to find reachable nodes
    vertices.iterate(|reach| {
        let edges_in_scope = edges.enter(&reach.scope());
        let reach_in_scope = reach.enter(&reach.scope());
        
        // Join current reachable nodes with edges to extend reachability
        let new_reachable = reach_in_scope
            .join_core(&edges_in_scope.map(|edge| (edge.src, edge.dst)), 
                        |src, (), dst| Some((*src, *dst)));
        
        // Add direct edge connections
        let direct_edges = edges_in_scope
            .map(|edge| (edge.src, edge.dst));
            
        new_reachable.concat(&direct_edges).distinct()
    })
}

/// Compute K-core of a graph iteratively
#[cfg(feature = "streaming")]
pub fn k_core<G: Scope>(
    edges: &Collection<G, Edge>,
    k: usize,
) -> Collection<G, Edge>
where
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    edges.iterate(|current_edges| {
        let edges_in_scope = edges.enter(&current_edges.scope());
        
        // Count degrees of vertices in current edges
        let degrees = current_edges
            .map(|edge| edge.src)
            .concat(&current_edges.map(|edge| edge.dst))
            .map(|v| (v, 1))
            .count();
        
        // Filter out vertices with degree < k
        let valid_vertices = degrees
            .filter(move |(_v, deg)| *deg >= k as isize)
            .map(|(v, _)| (v, ()));
        
        // Keep only edges between valid vertices
        let filtered_edges = edges_in_scope
            .map(|edge| ((edge.src, edge.dst), edge))
            .join_core(&valid_vertices, |(src, dst), edge, _| {
                Some((*src, (*dst, edge.clone())))
            })
            .map(|(src, (dst, edge))| ((src, dst), edge))
            .join_core(&valid_vertices, |(src, dst), edge, _| {
                Some((Edge { src: *src, dst: *dst, label: edge.label.clone() }, ()))
            });
            
        filtered_edges.map(|(edge, _)| edge)
    })
}

/// Simple PageRank implementation using iterative power method
#[cfg(feature = "streaming")]
pub fn pagerank<G: Scope>(
    edges: &Collection<G, Edge>,
    damping_factor: f64,
    iterations: usize,
) -> Collection<G, (VertexId, f64)>
where
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    // Get all unique vertices
    let vertices = edges
        .map(|edge| edge.src)
        .concat(&edges.map(|edge| edge.dst))
        .distinct();
        
    // Initial uniform rank (1/n for each vertex)
    let initial_ranks = vertices.map(|v| (v, 1.0));
    
    // Prepare edge contributions (src -> (dst, 1/out_degree))
    let out_degrees = edges
        .map(|edge| (edge.src, 1))
        .count();
        
    let edge_contributions = edges
        .map(|edge| edge.src)
        .join(&out_degrees)
        .map(|(src, (edge, out_deg))| {
            (edge.dst, (src, 1.0 / *out_deg as f64))
        });
    
    // Iterative power method
    initial_ranks.iterate(move |ranks| {
        let edge_contrib = edge_contributions.enter(&ranks.scope());
        
        // Distribute rank contributions
        let contributions = edge_contrib
            .join(&ranks)
            .map(|(dst, (src, contrib, rank))| {
                (dst, contrib * rank)
            });
            
        // Aggregate contributions
        let new_ranks = contributions
            .group(|_dst, s, t| {
                let total: f64 = s.iter().map(|(_, contrib, rank)| contrib * rank).sum();
                t.push((total, 1));
            })
            .map(|(dst, total_rank)| {
                // Apply damping factor: PR = (1-d) + d * sum(incoming_contributions)
                let page_rank = (1.0 - damping_factor) + damping_factor * total_rank;
                (dst, page_rank)
            });
            
        new_ranks
    })
}

/// Connected components using union-find approach
#[cfg(feature = "streaming")]
pub fn connected_components<G: Scope>(
    edges: &Collection<G, Edge>,
) -> Collection<G, (VertexId, VertexId)>
where
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    // Initial component assignment (each vertex is its own component)
    let vertices = edges
        .map(|edge| edge.src)
        .concat(&edges.map(|edge| edge.dst))
        .distinct();
        
    let initial_components = vertices.map(|v| (v, v));
    
    // Iterate to find minimum component for each vertex
    initial_components.iterate(|components| {
        let components_in_scope = components.enter(&components.scope());
        let edges_in_scope = edges.enter(&components.scope());
        
        // For each edge, connect components of its endpoints
        let edge_connections = edges_in_scope
            .map(|edge| edge.src)
            .join(&components_in_scope)
            .map(|(src, (edge, src_comp))| {
                (edge.dst, (src, src_comp))
            })
            .join(&components_in_scope)
            .map(|(dst, (src, src_comp, dst_comp))| {
                // Both vertices should be in the minimum of their components
                let min_comp = if src_comp < dst_comp { src_comp } else { dst_comp };
                (src, min_comp)
            })
            .concat(&components_in_scope);
            
        // Propagate minimum components
        edge_connections
            .group(|_v, s, t| {
                let min_comp = s.iter().map(|(_, comp)| *comp).min().unwrap();
                t.push((min_comp, 1));
            })
            .map(|(v, component)| (v, component))
    })
}

/// Triangle counting for undirected graphs
#[cfg(feature = "streaming")]
pub fn triangle_count<G: Scope>(
    edges: &Collection<G, Edge>,
) -> Collection<G, (VertexId, VertexId, VertexId)>
where
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    // Ensure edges are undirected (add reverse edges)
    let undirected_edges = edges
        .map(|edge| edge)
        .concat(&edges.map(|edge| Edge { src: edge.dst, dst: edge.src, label: edge.label }));
    
    // Find triangles: (u,v) edges that share neighbor w
    undirected_edges
        .map(|edge| (edge.src, edge.dst))
        .join_core(&undirected_edges.map(|edge| (edge.src, edge.dst)), 
                    |u, v, w| Some((*u, (*v, *w))))
        .filter(|(u, (v, w))| u != w && v != w) // Remove degenerate cases
        .map(|(u, (v, w))| (v, w, u)) // Reorder for consistency
        .distinct()
}

/// Breadth-first search from a single source
#[cfg(feature = "streaming")]
pub fn bfs<G: Scope>(
    edges: &Collection<G, Edge>,
    source: VertexId,
) -> Collection<G, (VertexId, usize)>
where
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    use differential_dataflow::operators::iterate::Variable;
    
    let (handle, bfs_data) = edges.scope().variable_collection::<(VertexId, usize), _>();
    
    // Initialize with source at distance 0
    handle.set(Collection::new(edges.scope()).insert((source, 0)));
    
    // Iterative BFS
    let result = bfs_data.iterate(|distances| {
        let edges_in_scope = edges.enter(&distances.scope());
        let distances_in_scope = distances.enter(&distances.scope());
        
        // Find new reachable vertices
        let new_distances = edges_in_scope
            .join_core(&distances_in_scope, 
                        |src, dst, current_dist| {
                Some((*dst, current_dist + 1))
            });
            
        // Keep minimum distance for each vertex
        distances_in_scope
            .concat(&new_distances)
            .reduce(|_v, s, t| {
                let min_dist = s.iter().map(|(_, dist)| *dist).min().unwrap();
                t.push((min_dist, 1));
            })
    });
    
    result
}

/// Strongly connected components using Kosaraju's algorithm
#[cfg(feature = "streaming")]
pub fn strongly_connected_components<G: Scope>(
    edges: &Collection<G, Edge>,
) -> Collection<G, (VertexId, VertexId)>
where
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    // First pass: forward reachability
    let vertices = edges
        .map(|edge| edge.src)
        .concat(&edges.map(|edge| edge.dst))
        .distinct();
        
    let forward_reach = reachability(edges);
    
    // Second pass: reverse graph reachability  
    let reverse_edges = edges
        .map(|edge| Edge { src: edge.dst, dst: edge.src, label: edge.label });
        
    let reverse_reach = reachability(&reverse_edges);
    
    // Two vertices are in same SCC if they can reach each other
    vertices
        .map(|v| (v, v))
        .join_core(&forward_reach, |v1, v2, reachable| {
            if *v2 == *v1 { return None; } // Skip self
            Some((*v2, (*v1, *reachable)))
        })
        .join_core(&reverse_reach, |v2, (v1, _), mutual_reachable| {
            if *mutual_reachable && v1 < v2 {
                Some((*v1, *v2)) // Return smaller vertex as component representative
            } else {
                None
            }
        })
        .map(|(v1, v2)| if v1 < v2 { (v1, v1) } else { (v2, v2) })
        .distinct()
}

#[cfg(not(feature = "streaming"))]
pub fn reachability<T>(_edges: T) -> Vec<(VertexId, VertexId)> {
    panic!("reachability requires the 'streaming' feature to be enabled");
}

#[cfg(not(feature = "streaming"))]  
pub fn k_core<T>(_edges: T, _k: usize) -> Vec<Edge> {
    panic!("k_core requires the 'streaming' feature to be enabled");
}

#[cfg(not(feature = "streaming"))]
pub fn pagerank<T>(_edges: T, _damping_factor: f64, _iterations: usize) -> Vec<(VertexId, f64)> {
    panic!("pagerank requires the 'streaming' feature to be enabled");
}

#[cfg(not(feature = "streaming"))]
pub fn connected_components<T>(_edges: T) -> Vec<(VertexId, VertexId)> {
    panic!("connected_components requires the 'streaming' feature to be enabled");
}

#[cfg(not(feature = "streaming"))]
pub fn triangle_count<T>(_edges: T) -> usize {
    panic!("triangle_count requires the 'streaming' feature to be enabled");
}

#[cfg(not(feature = "streaming"))]
pub fn bfs<T>(_edges: T, _source: VertexId) -> Vec<VertexId> {
    panic!("bfs requires the 'streaming' feature to be enabled");
}

#[cfg(not(feature = "streaming"))]
pub fn strongly_connected_components<T>(_edges: T) -> Vec<Vec<VertexId>> {
    panic!("strongly_connected_components requires the 'streaming' feature to be enabled");
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_core::props;

    #[test]
    fn test_edge_creation() {
        let edge = Edge::from_ids(1, 2, "friend");
        assert_eq!(edge.src, VertexId::new(1));
        assert_eq!(edge.dst, VertexId::new(2));
        assert_eq!(edge.label, "friend");
    }

    #[test]
    fn test_edge_reversal() {
        let edge = Edge::from_ids(1, 2, "friend");
        let reversed = edge.reversed();
        assert_eq!(reversed.src, VertexId::new(2));
        assert_eq!(reversed.dst, VertexId::new(1));
        assert_eq!(reversed.label, "friend");
    }

    #[test]
    fn test_property_creation() {
        let props = props::map(vec![
            ("name", "Alice"),
            ("age", 30i64),
            ("active", true),
        ]);
        
        assert_eq!(props.get("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(props.get("age").unwrap().as_int64(), Some(30));
        assert_eq!(props.get("active").unwrap().as_bool(), Some(true));
    }

    // Additional integration tests would require setting up differential dataflow
    // with proper input sessions and scopes
}

// Basic algorithms module (no streaming feature required)
pub mod basic;

// Re-export basic algorithm types
pub use basic::{
    PageRankResult,
    ConnectedComponentsResult,
    DijkstraResult,
    AlgorithmStats,
    compute_pagerank,
    find_connected_components,
    compute_shortest_path,
    run_with_timing,
};