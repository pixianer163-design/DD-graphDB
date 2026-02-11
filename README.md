# DD-GraphDB

A modular graph database built in Rust, featuring ACID transactions, GQL query support, materialized views with incremental computation, and graph algorithms.

## Architecture

```
graph/
  core/          - Foundational types (VertexId, Edge, PropertyValue)
  storage/       - Persistent storage with WAL and ACID transactions
  query/         - GQL query parser (Pest-based) and executor
  algorithms/    - Graph algorithms (PageRank, Dijkstra, connected components)
  collection/    - Differential dataflow collection wrappers
  server/        - HTTP/gRPC server support
  views/         - Materialized views, incremental engine, multi-level cache
```

## Quick Start

### Build

```bash
# Default build (core features)
cargo build

# Full feature build
cargo build --all-features
```

### Run

```bash
# Interactive database shell
cargo run --features views

# Demo
cargo run --bin graph_demo

# Query demo
cargo run --bin query_demo --features query

# Algorithm demo
cargo run --bin algo_demo
```

### Test

```bash
cargo test --workspace
```

## Features

| Feature | Flag | Description |
|---------|------|-------------|
| Core types | `core` (default) | VertexId, Edge, PropertyValue |
| Serialization | `serde` | Serde support for all types |
| Async I/O | `async` | Async storage operations |
| Streaming | `streaming` | Differential dataflow support |
| Query | `query` | GQL query parsing and execution |
| Views | `views` | Materialized views + incremental computation |
| SQL | `sql` | SQL query parser for views |
| Full | `full` | All features enabled |

## Core Capabilities

- **ACID Transactions** - Write-ahead log with snapshot isolation
- **GQL Queries** - Pattern matching, property filters, edge traversal
- **Graph Algorithms** - PageRank, shortest path, connected components, triangle counting
- **Materialized Views** - Pre-computed query results with automatic refresh
- **Incremental Computation** - Change propagation with dependency tracking
- **Multi-level Cache** - L1/L2/L3 caching with adaptive eviction
- **Query Router** - Intelligent routing to materialized views

## Example

```rust
use graph_core::{VertexId, Edge, props};
use graph_storage::{GraphStorage, GraphOperation};

let storage = GraphStorage::new("./data")?;
let mut txn = storage.begin_transaction()?;

txn.add_operation(GraphOperation::AddVertex {
    id: VertexId::new(1),
    properties: props::map(vec![("name", "Alice"), ("type", "Person")]),
});

txn.add_operation(GraphOperation::AddEdge {
    edge: Edge::new(VertexId::new(1), VertexId::new(2), "friend"),
    properties: props::map(vec![("since", 2024i64)]),
});

storage.commit_transaction(txn)?;
```

## License

MIT
