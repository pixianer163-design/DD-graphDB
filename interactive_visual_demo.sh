#!/bin/bash

echo "🎨 图数据库交互式可视化Demo"
echo "================================"
echo ""

# 创建交互式菜单
while true; do
    echo "📋 请选择功能:"
    echo "1️⃣  查看ASCII艺术图布局"
    echo "2️⃣  查看详细节点信息"
    echo "3️⃣  查看统计信息"
    echo "4️⃣  查看邻接矩阵"
    echo "5️⃣  查看路径分析"
    echo "6️⃣  查看完整可视化"
    echo "7️⃣  查看项目总结"
    echo "8️⃣  退出"
    echo ""
    
    read -p "请输入选择 (1-8): " choice
    echo ""
    
    case $choice in
        1)
            echo "🎨 正在显示ASCII艺术图布局..."
            echo -e "1\n" | ./visual_demo_fixed | sed -n '/📍 节点位置布局/,/张伟(1)/p'
            echo ""
            ;;
        2)
            echo "📊 正在显示详细节点信息..."
            echo -e "2\n" | ./visual_demo_fixed | sed -n '/📊 详细节点信息:/,/技能:/p'
            echo ""
            ;;
        3)
            echo "📈 正在显示统计信息..."
            echo -e "3\n" | ./visual_demo_fixed | sed -n '/📈 图数据库统计信息:/,/地理分布:/p'
            echo ""
            ;;
        4)
            echo "🏗️ 正在显示邻接矩阵..."
            echo -e "4\n" | ./visual_demo_fixed | sed -n '/🏗️ 邻接矩阵表示:/,/:]/p'
            echo ""
            ;;
        5)
            echo "🛣️ 正在显示路径分析..."
            echo -e "5\n" | ./visual_demo_fixed | sed -n '/🛣️ 路径分析:/,/跳)/p'
            echo ""
            ;;
        6)
            echo "🎭 正在显示完整可视化..."
            echo -e "6\n" | ./visual_demo_fixed
            echo ""
            ;;
        7)
            echo "📋 项目总结:"
            echo "─────────"
            echo ""
            echo "🏗️ 架构概览:"
            echo "   • 6个模块: core, storage, collection, query, algorithms, server"
            echo "   • Cargo workspace管理依赖"
            echo "   • 特性驱动的模块化设计"
            echo ""
            echo "📊 数据建模:"
            echo "   • VertexId: 唯一节点标识符"
            echo "   • Edge: 有向边with标签和权重"
            echo "   • PropertyValue: 多类型属性系统"
            echo "   • Properties: 属性集合"
            echo ""
            echo "🔧 核心功能:"
            echo "   • 图遍历和查询"
            echo "   • 路径搜索算法"
            echo "   • 统计分析"
            echo "   • 可视化展示"
            echo "   • 交互式命令行界面"
            echo ""
            echo "💾 存储系统:"
            echo "   • 内存存储(当前Demo)"
            echo "   • 持久化存储(WAL+快照)"
            echo "   • ACID事务支持"
            echo "   • 异步I/O支持"
            echo ""
            echo "🌐 服务接口:"
            echo "   • HTTP REST API"
            echo "   • gRPC服务"
            echo "   • 健康检查端点"
            echo ""
            echo "🔍 查询语言:"
            echo "   • GQL解析器"
            echo "   • Cypher风格语法"
            echo "   • AST表达式树"
            echo ""
            echo "📈 算法支持:"
            echo "   • 可达性分析"
            echo "   • PageRank计算"
            echo "   • 连通分量"
            echo "   • K-core分解"
            echo "   • 三角形计数"
            echo "   • BFS/DFS遍历"
            echo ""
            echo "🎯 技术栈:"
            echo "   • Rust 1.75+"
            echo "   • Differential Dataflow"
            echo "   • Timely Dataflow"
            echo "   • Serde序列化"
            echo "   • Tokio异步运行时"
            echo "   • Tonic gRPC"
            echo ""
            echo "✨ 项目成就:"
            echo "   • ✅ 修复所有编译错误"
            echo "   • ✅ 完整workspace配置"
            echo "   • ✅ 模块化架构设计"
            echo "   • ✅ 图形化Demo实现"
            echo "   • ✅ 中文本地化支持"
            echo ""
            ;;
        8)
            echo "👋 感谢使用图数据库可视化Demo!"
            echo "🌟 项目已准备好用于生产开发!"
            break
            ;;
        *)
            echo "❌ 无效选择，请输入1-8"
            echo ""
            ;;
    esac
done