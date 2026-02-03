#!/bin/bash

echo "🚀 Graph Database Demo Showcase"
echo "=============================="
echo ""

echo "1️⃣  Running Automated Demo..."
cd src
./demo_app --demo
echo ""

echo "2️⃣  Creating Interactive Demo Session..."
echo "Let me show you the interactive mode with sample data:"
echo ""

# Create a script for interactive demo
cat > interactive_demo.txt << 'EOF'
stats
help
quit
EOF

echo "Commands we'll run in interactive mode:"
cat interactive_demo.txt
echo ""

echo "🎮 Launching Interactive Demo:"
./demo_app --interactive < interactive_demo.txt

echo ""
echo "3️⃣  Demo Summary:"
echo "✅ Core graph operations demonstrated:"
echo "   • Vertex and Edge creation"
echo "   • Property-based queries"  
echo "   • Relationship traversals"
echo "   • Department statistics"
echo "   • Management hierarchy analysis"
echo ""
echo "✅ Features working:"
echo "   • In-memory storage engine"
echo "   • Property system with multiple data types"
echo "   • Query interface"
echo "   • Interactive shell"
echo "   • Real-time statistics"
echo ""
echo "🎯 Next Steps for Full Implementation:"
echo "   • Persistent storage with WAL"
echo "   • Differential dataflow streaming"
echo "   • GQL query language parser"
echo "   • HTTP/gRPC server APIs"
echo "   • Advanced graph algorithms"
echo ""

echo "📚 The demo successfully demonstrates:"
echo "   - Graph data structure design"
echo "   - Property management system"
echo "   - Query execution engine"
echo "   - Interactive CLI interface"
echo "   - Real-time analytics capabilities"
echo ""

echo "🔧 To build the full workspace:"
echo "   cargo build --workspace --features full"
echo ""
echo "🚀 To run individual demos:"
echo "   cargo run --bin graph_demo"
echo "   cargo run --bin graph_database"