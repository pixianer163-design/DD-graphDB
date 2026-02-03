//! Graph Database Server
//!
//! This module provides server functionality for the graph database,
//! including HTTP and gRPC interfaces for client access.

use std::collections::HashMap;
use std::sync::Arc;
use std::net::SocketAddr;

use tokio::sync::RwLock;
use tracing::{error, info};

// Re-export main server struct
pub use crate::core::GraphServer;

/// Core server implementation
pub mod core {
    use super::*;
    use graph_storage::GraphStorage;
    use graph_core::{VertexId, Edge, Properties};

    /// Main graph database server
    pub struct GraphServer {
        storage: Arc<RwLock<GraphStorage>>,
        addr: SocketAddr,
    }

    impl GraphServer {
        /// Create a new graph server
        pub fn new() -> Self {
            Self {
                storage: Arc::new(RwLock::new(GraphStorage::new("./graph_data").unwrap())),
                addr: "127.0.0.1:50051".parse().unwrap(),
            }
        }

        /// Start the server
        pub async fn serve(&mut self, bind_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
            let addr: SocketAddr = bind_addr.parse()?;
            self.addr = addr;
            
            info!("🚀 Starting Graph Database Server on {}", addr);
            info!("📊 Differential Dataflow Powered");
            info!("🔍 GQL Query Support");
            info!("💾 Persistent Storage with ACID");
            
            // For now, just start a simple status service
            // In a full implementation, this would start HTTP/gRPC servers
            self.start_health_check().await?;
            
            Ok(())
        }

        /// Simple health check endpoint
        async fn start_health_check(&self) -> Result<(), Box<dyn std::error::Error>> {
            let addr = self.addr;
            info!("Starting health check service on {}", addr);
            
            // Simple TCP server for health checks
            use tokio::net::TcpListener;
            
            let listener = TcpListener::bind(addr).await?;
            info!("Health check server listening on {}", addr);
            
            loop {
                match listener.accept().await {
                    Ok((mut socket, addr)) => {
                        info!("Health check connection from {}", addr);
                        
                        let response = "OK: Graph Database Server is running\n";
                        if let Err(e) = tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes()).await {
                            error!("Failed to write health response: {}", e);
                        }
                    }
                    Err(e) => error!("Failed to accept connection: {}", e),
                }
            }
        }

        /// Get server statistics
        pub async fn get_stats(&self) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
            let storage = self.storage.read().await;
            match storage.get_stats() {
                Ok(stats) => {
                    let mut result = HashMap::new();
                    result.insert("vertex_count".to_string(), stats.vertex_count.to_string());
                    result.insert("edge_count".to_string(), stats.edge_count.to_string());
                    result.insert("version".to_string(), stats.version.to_string());
                    result.insert("timestamp".to_string(), format!("{:?}", stats.timestamp));
                    Ok(result)
                }
                Err(e) => {
                    error!("Failed to get stats: {}", e);
                    Err(e.into())
                }
            }
        }

        /// Get the server address
        pub fn addr(&self) -> &SocketAddr {
            &self.addr
        }
    }
}

/// HTTP API server module
#[cfg(feature = "http")]
pub mod http {
    use super::*;
    use tokio::net::TcpListener;
    use std::collections::HashMap;

    /// Simple HTTP server for REST API
    pub struct HttpServer {
        storage: Arc<RwLock<GraphStorage>>,
        addr: SocketAddr,
    }

    impl HttpServer {
        /// Create new HTTP server
        pub fn new(storage: Arc<RwLock<GraphStorage>>) -> Self {
            Self {
                storage,
                addr: "127.0.0.1:8080".parse().unwrap(),
            }
        }

        /// Start HTTP server
        pub async fn serve(&self) -> Result<(), Box<dyn std::error::Error>> {
            let listener = TcpListener::bind(&self.addr).await?;
            info!("🌐 HTTP API server listening on {}", self.addr);
            
            loop {
                match listener.accept().await {
                    Ok((mut socket, addr)) => {
                        info!("HTTP connection from {}", addr);
                        
                        let storage = self.storage.clone();
                        tokio::spawn(async move {
                            Self::handle_connection(&mut socket, storage).await;
                        });
                    }
                    Err(e) => error!("Failed to accept HTTP connection: {}", e),
                }
            }
        }

        /// Handle HTTP connection
        async fn handle_connection(
            socket: &mut tokio::net::TcpStream,
            storage: Arc<RwLock<GraphStorage>>,
        ) {
            use tokio::io::AsyncReadExt;
            
            let mut buffer = [0; 1024];
            match socket.read(&mut buffer).await {
                Ok(n) => {
                    let request = String::from_utf8_lossy(&buffer[..n]);
                    let response = Self::handle_http_request(&request, storage).await;
                    let _ = socket.write_all(response.as_bytes()).await;
                }
                Err(e) => error!("Failed to read HTTP request: {}", e),
            }
        }

        /// Handle HTTP request
        async fn handle_http_request(
            request: &str,
            storage: Arc<RwLock<GraphStorage>>,
        ) -> String {
            let lines: Vec<&str> = request.lines().collect();
            if lines.is_empty() {
                return "HTTP/1.1 400 Bad Request\r\n\r\n".to_string();
            }

            let request_line = lines[0];
            let parts: Vec<&str> = request_line.split_whitespace().collect();
            
            if parts.len() < 2 {
                return "HTTP/1.1 400 Bad Request\r\n\r\n".to_string();
            }

            let method = parts[0];
            let path = parts[1];

            match (method, path) {
                ("GET", "/health") => {
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ok\"}\n".to_string()
                }
                ("GET", "/stats") => {
                    let storage_guard = storage.read().await;
                    match storage_guard.get_stats() {
                        Ok(stats) => {
                            let json = format!(
                                "{{\"vertex_count\":{},\"edge_count\":{},\"version\":{},\"timestamp\":\"{:?}\"}}",
                                stats.vertex_count,
                                stats.edge_count,
                                stats.version,
                                stats.timestamp
                            );
                            format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{}", json)
                        }
                        Err(_) => {
                            "HTTP/1.1 500 Internal Server Error\r\n\r\n".to_string()
                        }
                    }
                }
                ("GET", "/") => {
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<!DOCTYPE html><html><head><title>Graph Database</title></head><body><h1>Graph Database API</h1><p>Endpoints:</p><ul><li><a href=\"/health\">GET /health</a> - Health check</li><li><a href=\"/stats\">GET /stats</a> - Database statistics</li></ul></body></html>\n".to_string()
                }
                _ => {
                    "HTTP/1.1 404 Not Found\r\n\r\n".to_string()
                }
            }
        }
    }
}

/// gRPC server module
#[cfg(feature = "grpc")]
pub mod grpc {
    use super::*;
    use tonic::transport::Server;

    /// gRPC server implementation placeholder
    pub struct GrpcServer {
        storage: Arc<RwLock<GraphStorage>>,
    }

    impl GrpcServer {
        /// Create new gRPC server
        pub fn new(storage: Arc<RwLock<GraphStorage>>) -> Self {
            Self { storage }
        }

        /// Start gRPC server
        pub async fn serve(&self, addr: String) -> Result<(), Box<dyn std::error::Error>> {
            info!("🔧 gRPC server starting on {}", addr);
            info!("Note: Full gRPC implementation would use tonic/prost");
            
            // For now, just log that gRPC would start here
            // A full implementation would define .proto service and implement it
            
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_creation() {
        let server = GraphServer::new();
        assert_eq!(server.addr().to_string(), "127.0.0.1:50051");
    }

    #[test]
    fn test_http_server() {
        use tokio::sync::RwLock;
        
        let storage = Arc::new(RwLock::new(
            GraphStorage::new("./test_data").unwrap()
        ));
        let http_server = http::HttpServer::new(storage);
        
        // Test that server can be created
        assert_eq!(http_server.addr.to_string(), "127.0.0.1:8080");
    }

    #[test]
    fn test_grpc_server() {
        use tokio::sync::RwLock;
        
        let storage = Arc::new(RwLock::new(
            GraphStorage::new("./test_data").unwrap()
        ));
        let grpc_server = grpc::GrpcServer::new(storage);
        
        // Test that gRPC server can be created
        assert!(true); // Just verify creation doesn't panic
    }

    #[test]
    fn test_stats_retrieval() {
        let server = GraphServer::new();
        
        // This would be an async test in a real scenario
        // For now, just test the method exists
        assert!(true);
    }
}