//! Stream Source Manager for Graph Database
//!
//! This module provides a unified manager for all stream sources,
//! handling lifecycle, configuration, and coordination.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;

use super::{
    StreamEvent, StreamProcessor, StreamError, StreamProcessorConfig,
    StreamSource, StreamSourceConfig, StreamSourceType, ConnectionConfig,
    KafkaStreamSource, HttpWebhookSource, SourceStatus, SourceStats
};

/// Stream source manager
pub struct StreamSourceManager {
    /// Configured sources
    sources: Arc<RwLock<HashMap<String, Arc<RwLock<dyn StreamSource>>>>>,
    /// Stream processor
    stream_processor: Arc<StreamProcessor>,
    /// Event channel
    event_sender: mpsc::UnboundedSender<StreamEvent>,
    event_receiver: Arc<RwLock<Option<mpsc::UnboundedReceiver<StreamEvent>>>>,
    /// Manager configuration
    config: StreamSourceManagerConfig,
    /// Manager statistics
    stats: Arc<RwLock<StreamSourceManagerStats>>,
}

/// Stream source manager configuration
#[derive(Debug, Clone)]
pub struct StreamSourceManagerConfig {
    /// Maximum number of concurrent sources
    pub max_sources: usize,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Auto-restart failed sources
    pub auto_restart: bool,
    /// Restart delay
    pub restart_delay: Duration,
    /// Enable source discovery
    pub enable_discovery: bool,
    /// Discovery interval
    pub discovery_interval: Duration,
}

impl Default for StreamSourceManagerConfig {
    fn default() -> Self {
        Self {
            max_sources: 100,
            health_check_interval: Duration::from_secs(30),
            auto_restart: true,
            restart_delay: Duration::from_secs(10),
            enable_discovery: false,
            discovery_interval: Duration::from_secs(60),
        }
    }
}

/// Stream source manager statistics
#[derive(Debug, Clone, Default)]
pub struct StreamSourceManagerStats {
    /// Total sources managed
    pub total_sources: usize,
    /// Active sources
    pub active_sources: usize,
    /// Failed sources
    pub failed_sources: usize,
    /// Total events processed
    pub total_events_processed: u64,
    /// Events per second rate
    pub events_per_second: f64,
    /// Last health check time
    pub last_health_check: Option<SystemTime>,
    /// Total restarts
    pub total_restarts: u32,
}

impl StreamSourceManager {
    /// Create new stream source manager
    pub fn new(
        stream_processor: Arc<StreamProcessor>,
        config: StreamSourceManagerConfig,
    ) -> Self {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        
        Self {
            sources: Arc::new(RwLock::new(HashMap::new())),
            stream_processor,
            event_sender,
            event_receiver: Arc::new(RwLock::new(Some(event_receiver))),
            config,
            stats: Arc::new(RwLock::new(StreamSourceManagerStats::default())),
        }
    }

    /// Add a new stream source
    pub fn add_source(&self, source_config: StreamSourceConfig) -> Result<(), StreamError> {
        println!("🔌 Adding stream source: {}", source_config.source_id);

        // Check source limit
        {
            let sources = self.sources.read().unwrap();
            if sources.len() >= self.config.max_sources {
                return Err(StreamError::ProcessingFailed { 
                    error: format!("Maximum source limit ({}) reached", self.config.max_sources) 
                });
            }
        }

        // Create source based on type
        let source = self.create_source(source_config.clone())?;
        
        // Add to sources map
        {
            let mut sources = self.sources.write().unwrap();
            sources.insert(source_config.source_id.clone(), source);
        }

        // Start the source
        self.start_source(&source_config.source_id)?;

        println!("✅ Added stream source: {}", source_config.source_id);
        Ok(())
    }

    /// Remove a stream source
    pub fn remove_source(&self, source_id: &str) -> Result<(), StreamError> {
        println!("🔌 Removing stream source: {}", source_id);

        // Stop the source first
        self.stop_source(source_id)?;

        // Remove from sources map
        {
            let mut sources = self.sources.write().unwrap();
            sources.remove(source_id);
        }

        println!("✅ Removed stream source: {}", source_id);
        Ok(())
    }

    /// Start a specific source
    pub fn start_source(&self, source_id: &str) -> Result<(), StreamError> {
        let sources = self.sources.read().unwrap();
        
        if let Some(source) = sources.get(source_id) {
            let mut source_lock = source.write().unwrap();
            let event_sender = self.event_sender.clone();
            source_lock.start(event_sender)?;
            println!("✅ Started source: {}", source_id);
        } else {
            return Err(StreamError::ProcessingFailed { 
                error: format!("Source {} not found", source_id) 
            });
        }

        Ok(())
    }

    /// Stop a specific source
    pub fn stop_source(&self, source_id: &str) -> Result<(), StreamError> {
        let sources = self.sources.read().unwrap();
        
        if let Some(source) = sources.get(source_id) {
            let mut source_lock = source.write().unwrap();
            source_lock.stop()?;
            println!("🛑 Stopped source: {}", source_id);
        } else {
            return Err(StreamError::ProcessingFailed { 
                error: format!("Source {} not found", source_id) 
            });
        }

        Ok(())
    }

    /// Start all sources
    pub fn start_all_sources(&self) -> Result<(), StreamError> {
        println!("🚀 Starting all stream sources");

        let sources = self.sources.read().unwrap();
        for source_id in sources.keys() {
            if let Err(e) = self.start_source(source_id) {
                eprintln!("Failed to start source {}: {}", source_id, e);
            }
        }

        println!("✅ Started all stream sources");
        Ok(())
    }

    /// Stop all sources
    pub fn stop_all_sources(&self) -> Result<(), StreamError> {
        println!("🛑 Stopping all stream sources");

        let sources = self.sources.read().unwrap();
        for source_id in sources.keys() {
            if let Err(e) = self.stop_source(source_id) {
                eprintln!("Failed to stop source {}: {}", source_id, e);
            }
        }

        println!("✅ Stopped all stream sources");
        Ok(())
    }

    /// Get list of all source IDs
    pub fn get_source_ids(&self) -> Vec<String> {
        let sources = self.sources.read().unwrap();
        sources.keys().cloned().collect()
    }

    /// Get status of all sources
    pub fn get_all_source_status(&self) -> HashMap<String, SourceStatus> {
        let sources = self.sources.read().unwrap();
        let mut status_map = HashMap::new();
        
        for (source_id, source) in sources.iter() {
            let source_lock = source.read().unwrap();
            status_map.insert(source_id.clone(), source_lock.get_status());
        }
        
        status_map
    }

    /// Get statistics of all sources
    pub fn get_all_source_stats(&self) -> HashMap<String, SourceStats> {
        let sources = self.sources.read().unwrap();
        let mut stats_map = HashMap::new();
        
        for (source_id, source) in sources.iter() {
            let source_lock = source.read().unwrap();
            stats_map.insert(source_id.clone(), source_lock.get_stats());
        }
        
        stats_map
    }

    /// Get manager statistics
    pub fn get_manager_stats(&self) -> StreamSourceManagerStats {
        let sources = self.sources.read().unwrap();
        let mut stats = self.stats.write().unwrap();
        
        stats.total_sources = sources.len();
        stats.active_sources = sources.iter()
            .filter(|(_, source)| source.read().unwrap().is_running())
            .count();
        stats.failed_sources = stats.total_sources - stats.active_sources;
        
        stats.clone()
    }

    /// Perform health check on all sources
    pub async fn health_check(&self) -> Result<(), StreamError> {
        println!("🏥 Performing health check on all sources");

        let sources = self.sources.read().unwrap();
        let mut failed_sources = Vec::new();

        for (source_id, source) in sources.iter() {
            let source_lock = source.read().unwrap();
            let status = source_lock.get_status();
            
            if !status.is_connected {
                failed_sources.push(source_id.clone());
            }
        }

        if !failed_sources.is_empty() && self.config.auto_restart {
            println!("🔄 Attempting to restart {} failed sources", failed_sources.len());
            
            for source_id in failed_sources {
                if let Err(e) = self.restart_source(&source_id).await {
                    eprintln!("Failed to restart source {}: {}", source_id, e);
                }
            }
        }

        // Update last health check time
        {
            let mut stats = self.stats.write().unwrap();
            stats.last_health_check = Some(SystemTime::now());
        }

        println!("✅ Health check completed");
        Ok(())
    }

    /// Restart a failed source
    async fn restart_source(&self, source_id: &str) -> Result<(), StreamError> {
        println!("🔄 Restarting source: {}", source_id);

        // Stop first
        self.stop_source(source_id)?;
        
        // Wait for restart delay
        tokio::time::sleep(self.config.restart_delay).await;
        
        // Start again
        self.start_source(source_id)?;

        // Update restart count
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_restarts += 1;
        }

        println!("✅ Restarted source: {}", source_id);
        Ok(())
    }

    /// Create a source based on configuration
    fn create_source(&self, config: StreamSourceConfig) -> Result<Arc<RwLock<dyn StreamSource>>, StreamError> {
        let source: Arc<RwLock<dyn StreamSource>> = match config.source_type {
            StreamSourceType::Kafka => {
                Arc::new(RwLock::new(KafkaStreamSource::new(config)))
            },
            StreamSourceType::HttpWebhook => {
                Arc::new(RwLock::new(HttpWebhookSource::new(config)))
            },
            _ => {
                return Err(StreamError::ProcessingFailed { 
                    error: format!("Unsupported source type: {:?}", config.source_type) 
                });
            }
        };

        Ok(source)
    }

    /// Start the source manager
    pub async fn start(&self) -> Result<(), StreamError> {
        println!("🚀 Starting Stream Source Manager");

        // Start event processing loop
        self.start_event_processing_loop().await?;

        // Start health check loop
        if self.config.health_check_interval > Duration::from_secs(0) {
            self.start_health_check_loop().await?;
        }

        println!("✅ Stream Source Manager started");
        Ok(())
    }

    /// Stop the source manager
    pub async fn stop(&self) -> Result<(), StreamError> {
        println!("🛑 Stopping Stream Source Manager");

        // Stop all sources
        self.stop_all_sources()?;

        // Note: The event processing loop will stop when all sources are stopped
        // and the channel is closed

        println!("✅ Stream Source Manager stopped");
        Ok(())
    }

    /// Start event processing loop
    async fn start_event_processing_loop(&self) -> Result<(), StreamError> {
        println!("🔄 Starting event processing loop");

        let mut receiver = {
            let mut rx_lock = self.event_receiver.write().unwrap();
            rx_lock.take().ok_or_else(|| StreamError::ProcessingFailed { 
                error: "Event receiver already taken".to_string() 
            })?
        };

        let stream_processor = self.stream_processor.clone();
        let stats = self.stats.clone();

        tokio::spawn(async move {
            let mut event_count = 0u64;
            let mut last_stats_update = SystemTime::now();

            while let Some(event) = receiver.recv().await {
                // Process event through stream processor
                if let Err(e) = stream_processor.process_event(event).await {
                    eprintln!("Failed to process event: {}", e);
                }

                event_count += 1;

                // Update stats periodically
                if SystemTime::now().duration_since(last_stats_update).unwrap_or_default() >= Duration::from_secs(1) {
                    {
                        let mut st = stats.write().unwrap();
                        st.total_events_processed = event_count;
                        st.events_per_second = event_count as f64 / 
                            SystemTime::now().duration_since(last_stats_update).unwrap_or_default().as_secs_f64();
                    }
                    last_stats_update = SystemTime::now();
                }
            }
        });

        Ok(())
    }

    /// Start health check loop
    async fn start_health_check_loop(&self) -> Result<(), StreamError> {
        println!("🏥 Starting health check loop");

        let manager = self.clone();
        let interval = self.config.health_check_interval;

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                if let Err(e) = manager.health_check().await {
                    eprintln!("Health check failed: {}", e);
                }
            }
        });

        Ok(())
    }
}

// Implement Clone for StreamSourceManager to allow sharing between tasks
impl Clone for StreamSourceManager {
    fn clone(&self) -> Self {
        Self {
            sources: self.sources.clone(),
            stream_processor: self.stream_processor.clone(),
            event_sender: self.event_sender.clone(),
            event_receiver: self.event_receiver.clone(),
            config: self.config.clone(),
            stats: self.stats.clone(),
        }
    }
}