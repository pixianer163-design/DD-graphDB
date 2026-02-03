# Rust Workspace Best Practices for Graph Database

## 1. Cargo Workspace Configuration

### Workspace Structure
- **Resolver "2"**: Always use resolver 2 for workspaces to get proper feature resolution
- **Centralized dependencies**: Define all external dependencies in `[workspace.dependencies]`
- **Workspace metadata**: Use `[workspace.package]` for shared metadata

### Key Benefits
- Single source of truth for dependency versions
- Consistent feature flags across crates
- Simplified dependency management

## 2. Dependency Management Between Crates

### Dependency Hierarchy
```
graph_database/
├── graph-core (foundational types)
├── graph-storage (depends on core)
├── graph-collection (depends on core, storage)
├── graph-query (depends on core, collection, storage)
├── graph-algorithms (depends on core, collection)
└── graph-server (depends on all crates)
```

### Best Practices
- Use `path = "../crate-name"` for internal dependencies
- Avoid circular dependencies
- Keep dependency graph acyclic
- Use feature flags for optional dependencies

## 3. Feature Flags Organization

### Feature Flag Strategy
- **Default features**: Keep minimal, only essential functionality
- **Named features**: Group related functionality (e.g., "grpc", "streaming")
- **Transitive features**: Propagate features through dependency chain
- **Optional dependencies**: Use feature gates for heavy dependencies

### Example Usage
```bash
# Minimal build
cargo build --release

# With gRPC support
cargo build --features grpc

# Full streaming capabilities
cargo build --features streaming,grpc
```

## 4. Testing Strategies

### Workspace-Level Testing
```bash
# Test all crates
cargo test --workspace

# Test specific crate
cargo test -p graph-core

# Test with features
cargo test --features streaming

# Integration tests
cargo test --test integration_tests
```

### Test Organization
- **Unit tests**: In `src/` modules alongside code
- **Integration tests**: In `tests/` directory at workspace root
- **Performance tests**: In `benches/` directory
- **Property-based tests**: Use `proptest` for complex algorithms

## 5. Common Pitfalls to Avoid

### Dependency Management
1. **Version conflicts**: Always use workspace dependencies
2. **Feature explosion**: Keep default features minimal
3. **Circular dependencies**: Design clear dependency hierarchy
4. **Unused dependencies**: Regularly audit with `cargo machete`

### Performance Issues
1. **Large binaries**: Use feature flags, LTO optimization
2. **Slow compilation**: Avoid unnecessary dependencies in core crates
3. **Memory bloat**: Profile and optimize critical paths

### Development Workflow
1. **Inconsistent versions**: Use workspace metadata consistently
2. **Feature flag conflicts**: Test feature combinations
3. **Breaking changes**: Use semantic versioning across workspace

## 6. Recommended Tooling

### Essential Tools
```bash
# Dependency management
cargo install cargo-machete  # Find unused dependencies
cargo install cargo-audit    # Security vulnerability scanning
cargo install cargo-deny     # License and dependency checking

# Development
cargo install cargo-watch    # Auto-rebuild on changes
cargo install cargo-flamegraph # Performance profiling
cargo install cargo-expand   # Macro expansion debugging
```

### CI/CD Configuration
- Use `cargo test --workspace` for comprehensive testing
- Test multiple feature combinations
- Include dependency audits in CI pipeline
- Use `cargo clippy --workspace` for linting

## 7. Performance Optimization

### Build Optimizations
- Use LTO (Link Time Optimization) in release builds
- Enable codegen-units = 1 for maximum optimization
- Profile with `cargo flamegraph` to identify bottlenecks

### Runtime Optimizations
- Leverage Rust's zero-cost abstractions
- Use `unsafe` only when absolutely necessary
- Profile memory usage with `valgrind` or `heaptrack`

This configuration provides a solid foundation for scalable graph database development with proper dependency management, feature organization, and testing strategies.