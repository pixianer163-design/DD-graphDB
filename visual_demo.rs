//! Graph Database Visual Demo
//! 
//! Creates a visual representation of the graph database using ASCII art and terminal graphics

use std::collections::HashMap;
use std::io::{self, Write};

// Simple graph data structures
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct VertexId(u64);

impl VertexId {
    fn new(id: u64) -> Self { VertexId(id) }
}

impl std::fmt::Display for VertexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
struct Edge {
    src: VertexId,
    dst: VertexId,
    label: String,
    weight: f64,
}

impl Edge {
    fn new(src: VertexId, dst: VertexId, label: &str, weight: f64) -> Self {
        Edge {
            src, 
            dst, 
            label: label.to_string(),
            weight
        }
    }
}

#[derive(Debug, Clone)]
enum PropertyValue {
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Vec(Vec<PropertyValue>),
}

impl PropertyValue {
    fn string(s: &str) -> Self { PropertyValue::String(s.to_string()) }
    fn int64(i: i64) -> Self { PropertyValue::Int64(i) }
    fn float64(f: f64) -> Self { PropertyValue::Float64(f) }
    fn bool(b: bool) -> Self { PropertyValue::Bool(b) }
    fn vec(v: Vec<PropertyValue>) -> Self { PropertyValue::Vec(v) }
    
    fn as_string(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s),
            _ => None,
        }
    }
    
    fn as_int64(&self) -> Option<i64> {
        match self {
            PropertyValue::Int64(i) => Some(*i),
            _ => None,
        }
    }
}

impl std::fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropertyValue::String(s) => write!(f, "\"{}\"", s),
            PropertyValue::Int64(i) => write!(f, "{}", i),
            PropertyValue::Float64(fl) => write!(f, "{:.1}", fl),
            PropertyValue::Bool(b) => write!(f, "{}", b),
            PropertyValue::Vec(v) => {
                write!(f, "[")?;
                for (i, item) in v.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
        }
    }
}

type Properties = HashMap<String, PropertyValue>;

struct VisualGraph {
    vertices: HashMap<VertexId, Properties>,
    edges: Vec<Edge>,
}

impl VisualGraph {
    fn new() -> Self {
        Self {
            vertices: HashMap::new(),
            edges: Vec::new(),
        }
    }
    
    fn add_vertex(&mut self, id: VertexId, properties: Properties) {
        self.vertices.insert(id, properties);
    }
    
    fn add_edge(&mut self, edge: Edge) {
        self.edges.push(edge);
    }
    
    fn create_sample_social_network() -> Self {
        let mut graph = Self::new();
        
        // Add people with properties
        graph.add_vertex(VertexId::new(1), vec![
            ("name".to_string(), PropertyValue::string("张伟")),
            ("age".to_string(), PropertyValue::int64(32)),
            ("job".to_string(), PropertyValue::string("软件工程师")),
            ("location".to_string(), PropertyValue::string("北京")),
            ("skills".to_string(), PropertyValue::vec(vec![
                PropertyValue::string("Rust"),
                PropertyValue::string("Go"),
                PropertyValue::string("Python")
            ])),
        ].into_iter().collect());
        
        graph.add_vertex(VertexId::new(2), vec![
            ("name".to_string(), PropertyValue::string("李娜")),
            ("age".to_string(), PropertyValue::int64(28)),
            ("job".to_string(), PropertyValue::string("产品经理")),
            ("location".to_string(), PropertyValue::string("上海")),
            ("skills".to_string(), PropertyValue::vec(vec![
                PropertyValue::string("产品设计"),
                PropertyValue::string("数据分析")
            ])),
        ].into_iter().collect());
        
        graph.add_vertex(VertexId::new(3), vec![
            ("name".to_string(), PropertyValue::string("王强")),
            ("age".to_string(), PropertyValue::int64(35)),
            ("job".to_string(), PropertyValue::string("技术总监")),
            ("location".to_string(), PropertyValue::string("深圳")),
            ("skills".to_string(), PropertyValue::vec(vec![
                PropertyValue::string("架构设计"),
                PropertyValue::string("团队管理")
            ])),
        ].into_iter().collect());
        
        graph.add_vertex(VertexId::new(4), vec![
            ("name".to_string(), PropertyValue::string("刘芳")),
            ("age".to_string(), PropertyValue::int64(26)),
            ("job".to_string(), PropertyValue::string("UI设计师")),
            ("location".to_string(), PropertyValue::string("杭州")),
            ("skills".to_string(), PropertyValue::vec(vec![
                PropertyValue::string("Figma"),
                PropertyValue::string("Sketch")
            ])),
        ].into_iter().collect());
        
        graph.add_vertex(VertexId::new(5), vec![
            ("name".to_string(), PropertyValue::string("陈明")),
            ("age".to_string(), PropertyValue::int64(30)),
            ("job".to_string(), PropertyValue::string("数据科学家")),
            ("location".to_string(), PropertyValue::string("广州")),
            ("skills".to_string(), PropertyValue::vec(vec![
                PropertyValue::string("机器学习"),
                PropertyValue::string("R"),
                PropertyValue::string("TensorFlow")
            ])),
        ].into_iter().collect());
        
        graph.add_vertex(VertexId::new(6), vec![
            ("name".to_string(), PropertyValue::string("赵丽")),
            ("age".to_string(), PropertyValue::int64(29)),
            ("job".to_string(), PropertyValue::string("前端开发")),
            ("location".to_string(), PropertyValue::string("北京")),
            ("skills".to_string(), PropertyValue::vec(vec![
                PropertyValue::string("React"),
                PropertyValue::string("Vue"),
                PropertyValue::string("TypeScript")
            ])),
        ].into_iter().collect());
        
        // Add relationships
        graph.add_edge(Edge::new(VertexId::new(1), VertexId::new(2), "同事", 0.8));
        graph.add_edge(Edge::new(VertexId::new(1), VertexId::new(3), "下属", 0.9));
        graph.add_edge(Edge::new(VertexId::new(2), VertexId::new(4), "朋友", 0.7));
        graph.add_edge(Edge::new(VertexId::new(3), VertexId::new(1), "上司", 0.9));
        graph.add_edge(Edge::new(VertexId::new(3), VertexId::new(5), "合作伙伴", 0.6));
        graph.add_edge(Edge::new(VertexId::new(4), VertexId::new(6), "同学", 0.8));
        graph.add_edge(Edge::new(VertexId::new(5), VertexId::new(2), "项目合作", 0.7));
        graph.add_edge(Edge::new(VertexId::new(6), VertexId::new(1), "同事", 0.8));
        
        graph
    }
    
    fn render_ascii_art(&self) {
        println!("\n🎨 图数据库可视化展示");
        println!("═".repeat(80));
        
        // Create a simple ASCII layout
        println!("\n📍 节点位置布局 (简化版):");
        println!("    张伟(1)     ┌───────┐     李娜(2)");
        println!("       │         │       │         │");
        println!("       │         │同事(0.8)  │");
        println!("       │同事(0.8) │       │朋友(0.7) │");
        println!("       ▼         ▼       ▼         ▼");
        println!("    赵丽(6) ───同学(0.8)─── 刘芳(4)");
        println!("                                     ▲");
        println!("                                     │");
        println!("                               项目合作(0.7)");
        println!("                                     │");
        println!("                                     ▼");
        println!("                                  陈明(5)");
        println!("                                     ▲");
        println!("                                     │");
        println!("                               合作伙伴(0.6)");
        println!("                                     │");
        println!("                                     ▼");
        println!("                                  王强(3)");
        println!("                                     ▲");
        println!("                                     │");
        println!("                                   上司(0.9)");
        println!("                                     │");
        println!("                                     ▼");
        println!("                                   张伟(1)");
    }
    
    fn render_detailed_view(&self) {
        println!("\n📊 详细节点信息:");
        println!("─".repeat(80));
        
        for (id, props) in &self.vertices {
            println!("\n👤 节点 {}: ", id);
            println!("├─ 姓名: {}", props.get("name").unwrap_or(&PropertyValue::string("未知")));
            println!("├─ 年龄: {}", props.get("age").unwrap_or(&PropertyValue::int64(0)));
            println!("├─ 工作: {}", props.get("job").unwrap_or(&PropertyValue::string("未知")));
            println!("├─ 地点: {}", props.get("location").unwrap_or(&PropertyValue::string("未知")));
            
            if let Some(skills) = props.get("skills") {
                println!("└─ 技能: {}", skills);
            }
        }
        
        println!("\n🔗 关系网络:");
        println!("─".repeat(80));
        
        for (i, edge) in self.edges.iter().enumerate() {
            let src_name = self.vertices.get(&edge.src)
                .and_then(|p| p.get("name"))
                .and_then(|p| p.as_string())
                .unwrap_or("未知");
            let dst_name = self.vertices.get(&edge.dst)
                .and_then(|p| p.get("name"))
                .and_then(|p| p.as_string())
                .unwrap_or("未知");
            
            println!("{}. {} --[{} (权重: {:.1})]--> {}", 
                i + 1, src_name, edge.label, edge.weight, dst_name);
        }
    }
    
    fn render_statistics(&self) {
        println!("\n📈 图数据库统计信息:");
        println!("═".repeat(80));
        
        println!("👥 节点总数: {}", self.vertices.len());
        println!("🔗 边总数: {}", self.edges.len());
        
        // Calculate average degree
        let mut degree_counts = HashMap::new();
        for edge in &self.edges {
            *degree_counts.entry(edge.src).or_insert(0) += 1;
            *degree_counts.entry(edge.dst).or_insert(0) += 1;
        }
        
        if !self.vertices.is_empty() {
            let avg_degree = degree_counts.values().sum::<i32>() as f64 / self.vertices.len() as f64;
            println!("📊 平均度数: {:.2}", avg_degree);
        }
        
        // Count relationship types
        let mut rel_types = HashMap::new();
        for edge in &self.edges {
            *rel_types.entry(&edge.label).or_insert(0) += 1;
        }
        
        println!("\n🏷️  关系类型分布:");
        for (rel_type, count) in &rel_types {
            println!("   • {}: {} 个", rel_type, count);
        }
        
        // Find most connected person
        let max_degree = degree_counts.values().max();
        if let Some(&max_deg) = max_degree {
            let most_connected: Vec<_> = degree_counts.iter()
                .filter(|(_, &deg)| deg == max_deg)
                .collect();
            
            for (id, _) in most_connected {
                if let Some(props) = self.vertices.get(id) {
                    if let Some(name) = props.get("name").and_then(|p| p.as_string()) {
                        println!("🌟 最活跃节点: {} (度数: {})", name, max_deg);
                    }
                }
            }
        }
        
        // Location distribution
        let mut locations = HashMap::new();
        for props in self.vertices.values() {
            if let Some(location) = props.get("location").and_then(|p| p.as_string()) {
                *locations.entry(location).or_insert(0) += 1;
            }
        }
        
        println!("\n🌍 地理分布:");
        for (location, count) in &locations {
            println!("   • {}: {} 人", location, count);
        }
    }
    
    fn render_graph_matrix(&self) {
        println!("\n🏗️  邻接矩阵表示:");
        println!("═".repeat(80));
        
        let vertex_ids: Vec<_> = self.vertices.keys().copied().collect::<Vec<_>>();
        vertex_ids.sort();
        
        // Print header
        print!("        ");
        for id in &vertex_ids {
            print!("{:3} ", id.0);
        }
        println!();
        
        // Print matrix
        for row_id in &vertex_ids {
            print!("{:3} [", row_id.0);
            for col_id in &vertex_ids {
                let has_edge = self.edges.iter().any(|e| e.src == *row_id && e.dst == *col_id);
                if has_edge {
                    let edge = self.edges.iter().find(|e| e.src == *row_id && e.dst == *col_id).unwrap();
                    print!("{:3.1}", edge.weight);
                } else {
                    print!("  . ");
                }
            }
            println!(" ]");
        }
    }
    
    fn find_shortest_path(&self, from: VertexId, to: VertexId) -> Option<Vec<VertexId>> {
        use std::collections::{VecDeque, HashSet};
        
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut parent = HashMap::new();
        
        queue.push_back(from);
        visited.insert(from);
        
        while let Some(current) = queue.pop_front() {
            if current == to {
                // Reconstruct path
                let mut path = vec![to];
                while let Some(&p) = parent.get(&path.last().unwrap()) {
                    path.push(p);
                    if p == from { break; }
                }
                path.reverse();
                return Some(path);
            }
            
            // Find neighbors
            for edge in &self.edges {
                if edge.src == current {
                    let neighbor = edge.dst;
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        parent.insert(neighbor, current);
                        queue.push_back(neighbor);
                    }
                }
            }
        }
        
        None
    }
    
    fn render_path_analysis(&self) {
        println!("\n🛣️  路径分析:");
        println!("─".repeat(80));
        
        // Sample path queries
        let path_queries = vec![
            (VertexId::new(1), VertexId::new(4)), // 张伟 -> 刘芳
            (VertexId::new(6), VertexId::new(5)), // 赵丽 -> 陈明
            (VertexId::new(2), VertexId::new(3)), // 李娜 -> 王强
        ];
        
        for (from, to) in path_queries {
            let from_name = self.vertices.get(&from)
                .and_then(|p| p.get("name"))
                .and_then(|p| p.as_string())
                .unwrap_or("未知");
            let to_name = self.vertices.get(&to)
                .and_then(|p| p.get("name"))
                .and_then(|p| p.as_string())
                .unwrap_or("未知");
            
            if let Some(path) = self.find_shortest_path(from, to) {
                println!("🔍 路径: {} -> {}", from_name, to_name);
                for (i, node) in path.iter().enumerate() {
                    let node_name = self.vertices.get(node)
                        .and_then(|p| p.get("name"))
                        .and_then(|p| p.as_string())
                        .unwrap_or("未知");
                    
                    if i == 0 {
                        print!("   {}", node_name);
                    } else {
                        print!(" → {}", node_name);
                    }
                }
                println!(" (长度: {} 跳)", path.len() - 1);
            } else {
                println!("❌ 无路径: {} -> {}", from_name, to_name);
            }
        }
    }
}

fn main() {
    println!("🚀 图数据库可视化Demo");
    println!("💾 中文社交网络示例");
    
    let graph = VisualGraph::create_sample_social_network();
    
    // Different visualization modes
    println!("\n📋 可视化模式选择:");
    println!("1. ASCII 艺术布局");
    println!("2. 详细信息视图");
    println!("3. 统计信息");
    println!("4. 邻接矩阵");
    println!("5. 路径分析");
    println!("6. 全部展示");
    
    print!("\n请选择模式 (1-6, 默认6): ");
    io::stdout().flush().unwrap();
    
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    let choice = input.trim();
    
    match choice {
        "1" => graph.render_ascii_art(),
        "2" => graph.render_detailed_view(),
        "3" => graph.render_statistics(),
        "4" => graph.render_graph_matrix(),
        "5" => graph.render_path_analysis(),
        "6" | "" => {
            graph.render_ascii_art();
            graph.render_detailed_view();
            graph.render_statistics();
            graph.render_graph_matrix();
            graph.render_path_analysis();
        }
        _ => println!("❌ 无效选择"),
    }
    
    println!("\n✨ 可视化完成!");
    println!("🎯 这个Demo展示了图数据库的核心功能:");
    println!("   • 节点和边的存储");
    println!("   • 属性系统");
    println!("   • 关系建模");
    println!("   • 路径查找");
    println!("   • 统计分析");
    println!("   • 多种可视化方式");
}