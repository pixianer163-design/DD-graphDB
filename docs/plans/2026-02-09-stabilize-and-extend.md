# Stabilize, Fix Compilation, and Extend Graph Database Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all compilation errors across feature configurations, ensure all 39+ tests pass under all features, fix the broken main binary with `views` feature, and add integration tests for the query execution engine.

**Architecture:** The project is a Rust workspace with 7 crates. The default build (no features) compiles and 39 lib crate tests pass. However, the root binary (`src/main.rs`) fails to compile with `views` or `query` features due to type mismatches and missing fields. The `graph-collection` crate fails with `--all-features` due to misuse of `differential-dataflow` Collection API. This plan fixes all compilation issues first, then adds missing integration tests.

**Tech Stack:** Rust 2021, differential-dataflow 0.18, timely 0.25, tokio, axum, serde, pest 2.7

---

## Phase 1: Fix Root Binary Compilation (src/main.rs)

### Task 1: Fix GraphDatabaseServer struct initialization

**Files:**
- Modify: `src/main.rs:104-115`

**Step 1: Read the current GraphDatabaseServer struct definition and its `new()` method**

Read `src/main.rs` and find the `GraphDatabaseServer` struct definition and the `new()` function. Identify:
- All fields in the struct (look for `struct GraphDatabaseServer`)
- The `new()` function return (around line 60-120)
- The specific errors: missing field `view_registry`, `Option<Arc<>>` wrapping needed for `cache_manager` and `incremental_engine`

**Step 2: Fix the `new()` method to match struct definition**

The errors are:
1. `cache_manager` needs `Some()` wrapping (line ~111)
2. `incremental_engine` needs `Some()` wrapping (line ~113)
3. Missing field `view_registry` in initializer (line ~104)

Fix by:
- Wrapping `cache_manager` in `Some()`
- Wrapping `incremental_engine` in `Some()`
- Adding the missing `view_registry` field (initialize as `None` or create from existing components)

**Step 3: Fix the `warm_up_cache` mutability error**

At line ~786, `router.warm_up_cache(warm_patterns)` fails because `router` is `&` not `&mut`.
Fix by changing the method signature or obtaining a mutable reference.

**Step 4: Fix unused imports and variables**

- Remove unused import `ViewDataReader` (line ~27)
- Prefix unused variable `manager` with `_` (line ~485)

**Step 5: Run the build with views feature**

Run: `cargo build --features views`
Expected: Compiles successfully

**Step 6: Commit**

```bash
git add src/main.rs
git commit -m "fix: resolve compilation errors in main binary with views feature"
```

---

### Task 2: Fix stream_demo and test_sql_view binary compilation

**Files:**
- Modify: `src/main.rs` (or the specific binary source files if separate)
- Check: Any bin targets in `Cargo.toml` referencing broken code

**Step 1: Identify all failing binary targets**

Run: `cargo build --features "views,query" 2>&1 | grep "^error"` to list all remaining errors after Task 1 fixes.

**Step 2: Fix each binary target**

For each failing binary:
- Read the source file
- Fix type errors, missing imports, missing fields
- Ensure it compiles

**Step 3: Run full build with views+query features**

Run: `cargo build --features "views,query"`
Expected: All binaries compile

**Step 4: Commit**

```bash
git add -A src/ Cargo.toml
git commit -m "fix: resolve compilation errors in demo binaries"
```

---

## Phase 2: Fix graph-collection Crate (--all-features)

### Task 3: Fix differential-dataflow Collection API misuse

**Files:**
- Modify: `graph/collection/src/lib.rs:170-195`

**Step 1: Read the graph-collection source**

Read `graph/collection/src/lib.rs`. The errors are:
- Line ~175: Using `.filter()` on `Collection` as if it were an `Iterator` - `Collection` has its own `.filter()` method with different signature
- Line ~182: Type mismatch comparing `Timestamp` with `VertexId`
- Line ~189,195: Using `.count()` as Iterator method

**Step 2: Fix Collection API usage**

The `differential-dataflow` `Collection<G, D>` type has methods like `.filter()`, `.map()`, `.count()` but they operate on dataflow, not Iterator. The method signatures differ:
- `Collection::filter(predicate: impl Fn(&D) -> bool)` - works on data, not `(key, val)` tuples
- `Collection::count()` returns a `Collection` of `(key, count)`, not a single number

Fix by:
- Using correct `Collection` method signatures
- Removing Iterator-style chaining
- Using proper dataflow operators

**Step 3: Run build with all features**

Run: `cargo build --all-features`
Expected: Compiles (or at least graph-collection compiles)

**Step 4: Run tests**

Run: `cargo test -p graph-collection`
Expected: All tests pass

**Step 5: Commit**

```bash
git add graph/collection/src/lib.rs
git commit -m "fix: correct differential-dataflow Collection API usage in graph-collection"
```

---

## Phase 3: Add Integration Tests for Query Engine

### Task 4: Add query executor integration tests

**Files:**
- Modify: `graph/query/src/executor.rs` (add tests to existing test module)

**Step 1: Read current test module in executor.rs**

Read `graph/query/src/executor.rs` and find the existing `#[cfg(test)]` module. Currently has 3 tests.

**Step 2: Write failing test for property filter query**

Add test that creates a storage with vertices, runs a MATCH query with WHERE clause filtering by property, and asserts correct results:

```rust
#[test]
fn test_property_filter_query() {
    // Create storage with test data
    // Execute: MATCH (v:Person) WHERE v.age > 25 RETURN v
    // Assert: only vertices with age > 25 returned
}
```

**Step 3: Run test to verify it fails**

Run: `cargo test -p graph-query test_property_filter_query`
Expected: FAIL (test not yet fully implemented or function under test needs fixing)

**Step 4: Implement the test with real assertions**

Write the full test body using `QueryExecutor::new()` and `execute()`.

**Step 5: Run test to verify it passes**

Run: `cargo test -p graph-query test_property_filter_query`
Expected: PASS

**Step 6: Commit**

```bash
git add graph/query/src/executor.rs
git commit -m "test: add property filter integration test for query executor"
```

---

### Task 5: Add edge traversal and multi-condition query tests

**Files:**
- Modify: `graph/query/src/executor.rs`

**Step 1: Write failing test for edge traversal**

```rust
#[test]
fn test_edge_traversal_query() {
    // Create storage with vertices and edges
    // Execute: MATCH (a)-[e:manages]->(b) RETURN b
    // Assert: correct traversal results
}
```

**Step 2: Run to verify it fails**

Run: `cargo test -p graph-query test_edge_traversal_query`

**Step 3: Implement the full test**

**Step 4: Write test for multi-condition (AND/OR)**

```rust
#[test]
fn test_multi_condition_query() {
    // Execute: MATCH (v:Person) WHERE v.age > 25 AND v.department = 'Engineering' RETURN v
    // Assert: only matching vertices
}
```

**Step 5: Run all query tests**

Run: `cargo test -p graph-query`
Expected: All tests pass (existing 3 + new tests)

**Step 6: Commit**

```bash
git add graph/query/src/executor.rs
git commit -m "test: add edge traversal and multi-condition query tests"
```

---

## Phase 4: Add Storage Layer Tests

### Task 6: Add graph traversal method tests

**Files:**
- Modify: `graph/storage/src/lib.rs`

**Step 1: Read existing tests in storage**

Read `graph/storage/src/lib.rs` test module.

**Step 2: Write tests for traversal methods**

Add tests for:
- `get_out_neighbors` / `get_in_neighbors` / `get_all_neighbors`
- `traverse_1hop` / `traverse_2hop`
- `shortest_path`

**Step 3: Run tests**

Run: `cargo test -p graph-storage`
Expected: All tests pass

**Step 4: Commit**

```bash
git add graph/storage/src/lib.rs
git commit -m "test: add graph traversal method tests for storage layer"
```

---

## Phase 5: Full Verification

### Task 7: Full build and test verification

**Step 1: Run default build**

Run: `cargo build`
Expected: Success

**Step 2: Run build with views feature**

Run: `cargo build --features views`
Expected: Success

**Step 3: Run build with all features**

Run: `cargo build --all-features`
Expected: Success (or document known issues with streaming feature)

**Step 4: Run all workspace tests**

Run: `cargo test -p graph-core -p graph-storage -p graph-query -p graph-algorithms -p graph-server -p graph-views`
Expected: All 45+ tests pass

**Step 5: Run clippy**

Run: `cargo clippy --workspace 2>&1 | grep "^warning" | head -20`
Document any significant warnings.

**Step 6: Final commit**

```bash
git commit -m "chore: verify full build and test suite passes"
```

---

## Summary

| Phase | Tasks | Description |
|-------|-------|-------------|
| 1 | Task 1-2 | Fix main.rs and demo binary compilation with features |
| 2 | Task 3 | Fix graph-collection differential-dataflow API usage |
| 3 | Task 4-5 | Add query executor integration tests |
| 4 | Task 6 | Add storage traversal tests |
| 5 | Task 7 | Full verification across all features |

**Total: 7 tasks, estimated ~35 bite-sized steps**
