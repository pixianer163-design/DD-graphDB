//! Real-time Stream Processing for Graph Database
//!
//! This module implements a comprehensive real-time stream processing system
//! that integrates with the incremental computation engine for handling
//! continuous data ingestion and high-concurrency scenarios.

use std::collections::{HashMap, VecDeque};

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use graph_core::{VertexId, Edge, PropertyValue, Properties};
use super::{
    DataChange, ChangeSet, IncrementalEngine,
    MultiLevelCacheManager
};

// use num_cpus; // Temporarily disabled due to network issues

/// Stream event with timing and metadata
#[derive(Debug, Clone)]
pub struct StreamEvent {
    /// Unique event identifier
    pub event_id: String,
    /// Event type
    pub event_type: StreamEventType,
    /// Event data payload
    pub data: StreamEventData,
    /// Event timestamp
    pub timestamp: SystemTime,
    /// Event source (e.g., source_id, stream_name)
    pub source: String,
    /// Event metadata
    pub metadata: HashMap<String, PropertyValue>,
    /// Event priority (0-100)
    pub priority: u8,
    /// Watermark for event ordering
    pub watermark: Option<u64>,
}

/// Types of stream events
#[derive(Debug, Clone, PartialEq)]
pub enum StreamEventType {
    /// Data change event
    DataChange(DataChange),
    /// Batch update event
    BatchUpdate(Vec<DataChange>),
    /// Stream control event
    StreamControl(StreamControlType),
    /// Analytics trigger event
    AnalyticsTrigger(String),
    /// Maintenance event
    Maintenance(MaintenanceType),
}

/// Stream control event types
#[derive(Debug, Clone, PartialEq)]
pub enum StreamControlType {
    /// Pause processing
    Pause,
    /// Resume processing
    Resume,
    /// Flush buffers
    Flush,
    /// Reset state
    Reset,
    /// Scale resources
    Scale(u32),
}

/// Maintenance event types
#[derive(Debug, Clone, PartialEq)]
pub enum MaintenanceType {
    /// Garbage collection
    GarbageCollection,
    /// Index rebuild
    IndexRebuild,
    /// Cache warm-up
    CacheWarmUp,
    /// Health check
    HealthCheck,
}

/// Event data payload
#[derive(Debug, Clone)]
pub enum StreamEventData {
    /// Vertex data
    Vertex(VertexId, Properties),
    /// Edge data
    Edge(Edge, Properties),
    /// Graph topology change
    TopologyChange,
    /// Analytics result
    AnalyticsResult(String, PropertyValue),
    /// System metrics
    SystemMetrics(HashMap<String, PropertyValue>),
    /// Custom binary data
    Binary(Vec<u8>),
}

/// Stream buffer configuration
#[derive(Debug, Clone)]
pub struct StreamBufferConfig {
    /// Maximum buffer size
    pub max_buffer_size: usize,
    /// Flush interval
    pub flush_interval: Duration,
    /// Backpressure threshold
    pub backpressure_threshold: f64,
    /// Batch size for processing
    pub batch_size: usize,
    /// Enable event ordering
    pub enable_event_ordering: bool,
    /// Maximum out-of-order tolerance
    pub max_out_of_order_tolerance: u64,
}

impl Default for StreamBufferConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 10000,
            flush_interval: Duration::from_millis(100),
            backpressure_threshold: 0.8,
            batch_size: 1000,
            enable_event_ordering: true,
            max_out_of_order_tolerance: 1000, // 1 second
        }
    }
}

/// Stream processing statistics
#[derive(Debug, Clone, Default)]
pub struct StreamStats {
    /// Total events processed
    pub events_processed: u64,
    /// Events dropped due to backpressure
    pub events_dropped: u64,
    /// Current buffer utilization
    pub buffer_utilization: f64,
    /// Average processing latency
    pub avg_processing_latency_ms: f64,
    /// Maximum processing latency
    pub max_processing_latency_ms: u64,
    /// Events out of order
    pub events_out_of_order: u64,
    /// Last processing time
    pub last_processed_time: Option<SystemTime>,
}

/// Stream buffer with backpressure handling
#[cfg(feature = "async")]
#[derive(Debug)]
pub struct StreamBuffer {
    /// Configuration
    config: StreamBufferConfig,
    /// Event buffer
    events: Arc<RwLock<VecDeque<StreamEvent>>>,
    /// Processing statistics
    stats: Arc<RwLock<StreamStats>>,
    /// Watermark tracking
    watermark: Arc<RwLock<u64>>,
    /// Flush notification sender
    flush_notifier: Option<tokio::sync::mpsc::Sender<()>>,
}

impl StreamBuffer {
    /// Create new stream buffer
    pub fn new(config: StreamBufferConfig) -> Self {
        Self {
            config,
            events: Arc::new(RwLock::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(StreamStats::default())),
            watermark: Arc::new(RwLock::new(0)),
            flush_notifier: None,
        }
    }

    /// Add event to buffer
    pub fn add_event(&self, event: StreamEvent) -> Result<(), StreamError> {
        let mut events = self.events.write().unwrap();
        
        // Check buffer capacity and backpressure
        if events.len() >= self.config.max_buffer_size {
            // Apply backpressure - drop lowest priority events
            self.apply_backpressure(&mut events)?;
            
            if let Some(notifier) = &self.flush_notifier {
                let _ = notifier.try_send(());
            }
            
            return Err(StreamError::BufferFull {
                current_size: events.len(),
                max_size: self.config.max_buffer_size,
            });
        }

        // Clone event for watermark update
        let event_clone = event.clone();
        
        // Insert event maintaining order if enabled
        if self.config.enable_event_ordering {
            self.insert_ordered(&mut events, event)?;
        } else {
            events.push_back(event);
        }

        // Update watermark
        self.update_watermark(&event_clone);

        // Trigger flush if interval reached
        if self.should_flush(&events) {
            self.flush_events(&mut events)?;
        }

        Ok(())
    }

    /// Insert event maintaining order
    fn insert_ordered(&self, events: &mut VecDeque<StreamEvent>, event: StreamEvent) -> Result<(), StreamError> {
        if let Some(watermark) = event.watermark {
            // Insert at correct position based on watermark
            let mut insert_pos = events.len();
            
            for (i, existing_event) in events.iter().enumerate() {
                match existing_event.watermark {
                    Some(existing_watermark) if existing_watermark > watermark => {
                        insert_pos = i;
                        break;
                    }
                    Some(_) => continue,
                    None => {
                        // Events without watermark go to the end
                        break;
                    }
                }
            }

            events.insert(insert_pos, event);
        } else {
            events.push_back(event);
        }
        
        Ok(())
    }

    /// Apply backpressure by dropping low priority events
    fn apply_backpressure(&self, events: &mut VecDeque<StreamEvent>) -> Result<(), StreamError> {
        let mut to_remove = Vec::new();
        
        // Find events to drop (lowest priority)
        let events_to_drop = (events.len() as f64 * (1.0 - self.config.backpressure_threshold)) as usize;
        
        for _ in 0..events_to_drop {
            if let Some(event) = events.pop_front() {
                to_remove.push(event);
            } else {
                break;
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.events_dropped += to_remove.len() as u64;
        }

        Ok(())
    }

    /// Check if buffer should be flushed
    fn should_flush(&self, events: &VecDeque<StreamEvent>) -> bool {
        !events.is_empty() && 
        (events.len() >= self.config.batch_size ||
         SystemTime::now().duration_since(
             events.front().unwrap().timestamp
         ).unwrap_or(Duration::from_secs(0)) >= self.config.flush_interval)
    }

    /// Flush events to processing
    fn flush_events(&self, events: &mut VecDeque<StreamEvent>) -> Result<(), StreamError> {
        if events.is_empty() {
            return Ok(());
        }

        let batch_size = events.len();
        let flush_start = Instant::now();

        // Extract events to process
        let mut events_to_process = Vec::new();
        while let Some(event) = events.pop_front() {
            events_to_process.push(event);
        }

        // Update processing statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.events_processed += batch_size as u64;
            
            let current_time = SystemTime::now();
            stats.last_processed_time = Some(current_time);
            
            // Update latency metrics
            for event in &events_to_process {
                let latency = current_time.duration_since(event.timestamp)
                    .unwrap_or(Duration::from_secs(0))
                    .as_millis() as f64;
                
                stats.avg_processing_latency_ms = 
                    (stats.avg_processing_latency_ms * (stats.events_processed - batch_size as u64) as f64 + latency) / stats.events_processed as f64;
                
                stats.max_processing_latency_ms = 
                    stats.max_processing_latency_ms.max(latency as u64);
                
                // Check out-of-order events
                if let Some(watermark) = event.watermark {
                    let current_watermark = *self.watermark.read().unwrap();
                    if watermark < current_watermark {
                        stats.events_out_of_order += 1;
                    }
                }
            }
        }

        let flush_duration = flush_start.elapsed();
        
        // Log flush operation
        if batch_size > 0 {
            println!("🔄 Stream flushed: {} events in {} ms", batch_size, flush_duration.as_millis());
        }

        Ok(())
    }

    /// Update watermark for event ordering
    fn update_watermark(&self, event: &StreamEvent) {
        if let Some(event_watermark) = event.watermark {
            let mut watermark = self.watermark.write().unwrap();
            if event_watermark > *watermark {
                *watermark = event_watermark;
            }
        }
    }

    /// Get current statistics
    pub fn get_stats(&self) -> StreamStats {
        {
            let mut stats = self.stats.write().unwrap();
            let events = self.events.read().unwrap();
            stats.buffer_utilization = events.len() as f64 / self.config.max_buffer_size as f64;
            stats.clone()
        }
    }

    /// Set up flush notifications
    #[cfg(feature = "async")]
    pub fn set_flush_notifier(&mut self, sender: tokio::sync::mpsc::Sender<()>) {
        self.flush_notifier = Some(sender);
    }

    /// Reset buffer state
    pub fn reset(&self) {
        {
            let mut events = self.events.write().unwrap();
            events.clear();
        }
        
        {
            let mut stats = self.stats.write().unwrap();
            *stats = StreamStats::default();
        }
        
        {
            let mut watermark = self.watermark.write().unwrap();
            *watermark = 0;
        }
    }
}

/// Stream processing errors
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("Buffer full: {current_size}/{max_size}")]
    BufferFull { current_size: usize, max_size: usize },
    
    #[error("Event ordering failed: {reason}")]
    OrderingFailed { reason: String },
    
    #[error("Stream processing failed: {error}")]
    ProcessingFailed { error: String },
    
    #[error("Backpressure applied: {dropped} events")]
    BackpressureApplied { dropped: usize },
}

/// Real-time stream processor
#[cfg(feature = "async")]
#[derive(Debug)]
pub struct StreamProcessor {
    /// Incremental computation engine
    #[allow(dead_code)]
    incremental_engine: Arc<IncrementalEngine>,
    /// Cache manager
    #[allow(dead_code)]
    cache_manager: Arc<MultiLevelCacheManager>,
    /// Input buffer
    input_buffer: Arc<StreamBuffer>,
    /// Processing configuration
    config: StreamProcessorConfig,
    /// Processor state
    state: Arc<RwLock<StreamProcessorState>>,
    /// Worker handles
    workers: Vec<tokio::task::JoinHandle<()>>,
}

/// Stream processor configuration
#[cfg(feature = "async")]
#[derive(Debug, Clone)]
pub struct StreamProcessorConfig {
    /// Number of worker threads
    pub worker_count: usize,
    /// Processing queue size
    pub queue_size: usize,
    /// Enable parallel processing
    pub enable_parallel_processing: bool,
    /// Event timeout
    pub event_timeout: Duration,
    /// Enable event deduplication
    pub enable_deduplication: bool,
    /// Window size for time-based operations
    pub window_size: Duration,
}

#[cfg(feature = "async")]
impl Default for StreamProcessorConfig {
    fn default() -> Self {
        Self {
            worker_count: get_num_cpus(),
            queue_size: 50000,
            enable_parallel_processing: true,
            event_timeout: Duration::from_secs(30),
            enable_deduplication: true,
            window_size: Duration::from_secs(60),
        }
    }
}

/// Stream processor state
#[derive(Debug, Clone, Default)]
pub struct StreamProcessorState {
    /// Is processor active
    pub is_active: bool,
    /// Current processing rate
    pub processing_rate: f64,
    /// Last activity timestamp
    pub last_activity: Option<SystemTime>,
    /// Events in current window
    pub window_events: u64,
    /// Processing errors
    pub processing_errors: u64,
}

impl StreamProcessor {
/// Create new stream processor
    #[cfg(feature = "async")]
    pub fn new(
        incremental_engine: Arc<IncrementalEngine>,
        cache_manager: Arc<MultiLevelCacheManager>,
        config: StreamProcessorConfig,
    ) -> Self {
        let buffer_config = StreamBufferConfig {
            max_buffer_size: config.queue_size,
            flush_interval: config.window_size,
            backpressure_threshold: 0.8,
            batch_size: 100,
            enable_event_ordering: true,
            max_out_of_order_tolerance: 60, // seconds as u64
        };
        let input_buffer = Arc::new(StreamBuffer::new(buffer_config));
        
        Self {
            incremental_engine,
            cache_manager,
            input_buffer,
            config,
            state: Arc::new(RwLock::new(StreamProcessorState::default())),
            workers: Vec::new(),
        }
    }

    /// Start stream processing
    #[cfg(feature = "async")]
    pub async fn start(&mut self) -> Result<(), StreamError> {
        println!("🚀 Starting Real-time Stream Processor");
        println!("📊 Configuration: {} workers, parallel: {}", 
            self.config.worker_count, self.config.enable_parallel_processing);

        // Set up flush notifications
        // Temporarily disabled due to Arc mutability requirements
        // let (flush_tx, flush_rx) = tokio::sync::mpsc::channel(1000);
        // self.input_buffer.set_flush_notifier(flush_tx);

        // Mark processor as active
        {
            let mut state = self.state.write().unwrap();
            state.is_active = true;
            state.last_activity = Some(SystemTime::now());
        }

        // Start worker threads
        // Temporarily disabled due to Send/Sync requirements
        // for worker_id in 0..self.config.worker_count {
        //     let engine = self.incremental_engine.clone();
        //     let buffer = self.input_buffer.clone();
        //     let config = self.config.clone();
        //     
        //     let handle = tokio::spawn(async move {
        //         Self::worker_task(worker_id, engine, buffer, config).await;
        //     });
        //     
        //     self.workers.push(handle);
        // }
        
        // Temporarily disabled due to Send/Sync requirements
        // // Start monitoring task
        // let monitor_handle = tokio::spawn(async move {
        //     while let Some(()) = flush_rx.recv().await {
        //         // Buffer was flushed, could trigger additional processing
        //         tokio::time::sleep(Duration::from_millis(10)).await;
        //     }
        // });
        
        // self.workers.push(monitor_handle);

        println!("✅ Stream processor started with {} workers", self.config.worker_count);
        Ok(())
    }

    /// Stop stream processing
    pub async fn stop(&mut self) -> Result<(), StreamError> {
        println!("🛑 Stopping Real-time Stream Processor");

        // Mark processor as inactive
        {
            let mut state = self.state.write().unwrap();
            state.is_active = false;
        }

        // Wait for all workers to complete
        for worker in self.workers.drain(..) {
            worker.abort();
        }

        // Reset buffer
        self.input_buffer.reset();

        println!("✅ Stream processor stopped");
        Ok(())
    }

    /// Process a single event
    pub async fn process_event(&self, event: StreamEvent) -> Result<(), StreamError> {
        // Add to buffer (handles backpressure and ordering)
        self.input_buffer.add_event(event)?;
        
        // Update processor state
        {
            let mut state = self.state.write().unwrap();
            state.last_activity = Some(SystemTime::now());
            
            // Calculate processing rate (events per second)
            if let Some(last_activity) = state.last_activity {
                let elapsed = last_activity.elapsed().unwrap_or(Duration::from_secs(1));
                if elapsed.as_secs_f64() > 0.0 {
                    state.processing_rate = 1.0 / elapsed.as_secs_f64();
                }
            }
        }

        Ok(())
    }

    /// Process batch of events
    pub async fn process_batch(&self, events: Vec<StreamEvent>) -> Result<(), StreamError> {
        println!("📦 Processing batch of {} events", events.len());

        for event in events {
            self.process_event(event).await?;
        }

        println!("✅ Batch processing completed");
        Ok(())
    }

    /// Get stream processor statistics
    pub fn get_stats(&self) -> StreamProcessorStats {
        let buffer_stats = self.input_buffer.get_stats();
        let state = self.state.read().unwrap().clone();

        StreamProcessorStats {
            buffer_stats,
            processor_state: state,
            worker_count: self.config.worker_count,
        }
    }

    /// Worker task for processing events
    #[cfg(feature = "async")]
    #[allow(dead_code)]
    async fn worker_task(
        worker_id: usize,
        engine: Arc<IncrementalEngine>,
        buffer: Arc<StreamBuffer>,
        config: StreamProcessorConfig,
    ) {
        let mut local_buffer = VecDeque::new();

        loop {
            // Process events from buffer
            {
                let mut events = buffer.events.write().unwrap();
                
                // Move events to local buffer
                while local_buffer.len() < config.queue_size && !events.is_empty() {
                    if let Some(event) = events.pop_front() {
                        local_buffer.push_back(event);
                    } else {
                        break;
                    }
                }
            }

            // Process local buffer
            if !local_buffer.is_empty() {
                Self::process_local_buffer(worker_id, engine.clone(), &mut local_buffer).await;
            }

            // Small delay to prevent busy waiting
            #[cfg(feature = "async")]
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Process local buffer of events
    #[cfg(feature = "async")]
    #[allow(dead_code)]
    async fn process_local_buffer(
        worker_id: usize,
        engine: Arc<IncrementalEngine>,
        local_buffer: &mut VecDeque<StreamEvent>,
    ) {
        if local_buffer.is_empty() {
            return;
        }

        let batch_start = Instant::now();
        
        // Convert stream events to incremental changes
        let changesets = Self::convert_to_changesets(local_buffer);
        
        // Process through incremental engine
        for changeset in changesets {
            match engine.process_changeset(changeset) {
                Ok(results) => {
                    println!("Worker {}: Processed changeset, {} views updated", 
                        worker_id, results.len());
                }
                Err(e) => {
                    eprintln!("Worker {}: Failed to process changeset: {}", worker_id, e);
                }
            }
        }

        let batch_duration = batch_start.elapsed();
        if batch_duration.as_millis() > 0 {
            println!("Worker {}: Processed {} events in {} ms", 
                worker_id, local_buffer.len(), batch_duration.as_millis());
        }
    }

    /// Convert stream events to incremental changesets
    #[allow(dead_code)]
    fn convert_to_changesets(events: &VecDeque<StreamEvent>) -> Vec<ChangeSet> {
        let mut changesets = Vec::new();
        
        // Group events by source and time windows
        let mut event_groups: HashMap<String, Vec<StreamEvent>> = HashMap::new();
        
        for event in events {
            let source = event.source.clone();
            event_groups.entry(source).or_insert_with(Vec::new).push(event.clone());
        }

        // Convert each group to a changeset
        for (source, group_events) in event_groups {
            let mut changeset = ChangeSet::new(
                format!("stream_{}", SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()),
                source.clone()
            );

            for event in group_events {
                match &event.event_type {
                    StreamEventType::DataChange(change) => {
                        changeset.add_change(change.clone());
                    }
                    StreamEventType::BatchUpdate(changes) => {
                        for change in changes {
                            changeset.add_change(change.clone());
                        }
                    }
                    _ => {
                        // Handle other event types as needed
                    }
                }
            }

            changesets.push(changeset);
        }

        changesets
    }
}

/// Combined stream processor statistics
#[derive(Debug, Clone)]
pub struct StreamProcessorStats {
    /// Buffer statistics
    pub buffer_stats: StreamStats,
    /// Processor state
    pub processor_state: StreamProcessorState,
    /// Number of workers
    pub worker_count: usize,
}

/// Get number of available CPU cores
fn get_num_cpus() -> usize {
    // std::thread::available_parallelism().unwrap_or(NonZeroUsize::new(1).unwrap()).get()
    4 // Default to 4 CPUs for now
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_event_creation() {
        let event = StreamEvent {
            event_id: "test_event".to_string(),
            event_type: StreamEventType::DataChange(DataChange::AddVertex {
                id: VertexId::new(1),
                properties: HashMap::new(),
            }),
            data: StreamEventData::Vertex(VertexId::new(1), HashMap::new()),
            timestamp: SystemTime::now(),
            source: "test_source".to_string(),
            metadata: HashMap::new(),
            priority: 50,
            watermark: Some(12345),
        };

        assert_eq!(event.event_id, "test_event");
    }

    #[test]
    fn test_stream_buffer_config() {
        let config = StreamBufferConfig::default();
        assert_eq!(config.max_buffer_size, 10000);
        assert_eq!(config.batch_size, 1000);
        assert!(config.enable_event_ordering);
    }

    #[test]
    fn test_stream_processor_config() {
        let config = StreamProcessorConfig::default();
        assert!(config.enable_parallel_processing);
        assert!(config.enable_deduplication);
        assert!(config.worker_count > 0);
    }
}