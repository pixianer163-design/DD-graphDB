#!/bin/bash

# 图数据库检查点机制测试脚本

echo "🧪 测试图数据库检查点机制"
echo "================================"

# 创建测试目录
TEST_DIR="/tmp/checkpoint_test"
rm -rf $TEST_DIR
mkdir -p $TEST_DIR

echo "1️⃣ 运行演示创建初始数据..."
echo -e "demo\nstats\nquit" | ./target/debug/graph_database $TEST_DIR

echo ""
echo "2️⃣ 检查存储目录内容:"
ls -la $TEST_DIR/

echo ""
echo "3️⃣ 运行第二次演示触发更多操作..."
echo -e "demo\nstats\nquit" | ./target/debug/graph_database $TEST_DIR

echo ""
echo "4️⃣ 再次检查存储目录:"
ls -la $TEST_DIR/

echo ""
echo "5️⃣ 查看文件详情:"
if [ -f "$TEST_DIR/graph.wal" ]; then
    echo "WAL文件大小: $(wc -c < $TEST_DIR/graph.wal) 字节"
fi

if [ -f "$TEST_DIR/graph.snap" ]; then
    echo "快照文件大小: $(wc -c < $TEST_DIR/graph.snap) 字节"
fi

echo ""
echo "✅ 检查点机制测试完成！"