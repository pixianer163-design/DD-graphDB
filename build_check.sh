#!/bin/bash

echo "🔧 Graph Database Build Status Check"
echo "=================================="

# Check if all Cargo.toml files exist
echo "📁 Checking workspace structure..."
find graph -name "Cargo.toml" | wc -l | xargs echo "Crates found:"

# Check core compilation
echo "🦀 Checking core crate compilation..."
cd graph/core
rustc --crate-type lib src/lib.rs --edition 2021 --extern serde=/dev/null --extern thiserror=/dev/null --extern anyhow=/dev/null 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✅ Core compiles successfully"
else
    echo "❌ Core compilation failed"
fi

cd ../..

# Check workspace root
echo "📋 Workspace members:"
grep -A 10 "\[workspace\]" Cargo.toml | grep "members"

echo ""
echo "🚧 Build Issues Resolved:"
echo "- Fixed optional dependencies in workspace config"
echo "- Added proper feature flags for streaming, serde, async, grpc"
echo "- Fixed duplicate dependency declarations"
echo "- Added missing prost dependency for gRPC"
echo ""
echo "🎯 Next Steps:"
echo "- Run 'cargo build --workspace' to test full build"
echo "- Use 'cargo build --features streaming' for streaming features"
echo "- Use 'cargo build --features grpc' for gRPC server"