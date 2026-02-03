//! Stream Transformation and Enrichment Pipeline
//!
//! This module provides a comprehensive pipeline for transforming and enriching
//! streaming data before it reaches the graph database.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use regex::Regex;

use graph_core::{VertexId, Edge, PropertyValue, Properties};
use super::{
    StreamEvent, StreamEventType, StreamEventData, StreamError,
    ExtractionRule, TransformOperation, FilterOperator
};

/// Stream transformation pipeline
pub struct StreamTransformPipeline {
    /// Transformation stages
    stages: Vec<Box<dyn TransformStage>>,
    /// Pipeline configuration
    config: PipelineConfig,
    /// Pipeline statistics
    stats: Arc<RwLock<PipelineStats>>,
}

/// Pipeline configuration
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Enable parallel processing
    pub enable_parallel: bool,
    /// Maximum concurrent transformations
    pub max_concurrent: usize,
    /// Enable error recovery
    pub enable_error_recovery: bool,
    /// Dead letter queue for failed events
    pub dead_letter_queue: bool,
    /// Buffer size for staging
    pub buffer_size: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            enable_parallel: true,
            max_concurrent: 10,
            enable_error_recovery: true,
            dead_letter_queue: true,
            buffer_size: 1000,
        }
    }
}

/// Pipeline statistics
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    /// Events processed
    pub events_processed: u64,
    /// Events transformed
    pub events_transformed: u64,
    /// Events enriched
    pub events_enriched: u64,
    /// Events filtered out
    pub events_filtered: u64,
    /// Events failed
    pub events_failed: u64,
    /// Average processing time (microseconds)
    pub avg_processing_time_us: f64,
    /// Last processing time
    pub last_processed: Option<SystemTime>,
}

/// Transform stage trait
pub trait TransformStage: Send + Sync {
    /// Transform an event
    fn transform(&self, event: StreamEvent) -> Result<TransformResult, StreamError>;
    
    /// Get stage name
    fn get_name(&self) -> &str;
    
    /// Get stage statistics
    fn get_stats(&self) -> StageStats;
}

/// Result of a transformation
#[derive(Debug)]
pub struct TransformResult {
    /// Transformed event
    pub event: Option<StreamEvent>,
    /// Whether to continue processing
    pub continue_pipeline: bool,
    /// Transformation metadata
    pub metadata: HashMap<String, String>,
}

/// Stage statistics
#[derive(Debug, Clone, Default)]
pub struct StageStats {
    /// Events processed by stage
    pub events_processed: u64,
    /// Events successful
    pub events_successful: u64,
    /// Events failed
    pub events_failed: u64,
    /// Processing time
    pub total_processing_time_us: u64,
}

impl StreamTransformPipeline {
    /// Create new transformation pipeline
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            stages: Vec::new(),
            config,
            stats: Arc::new(RwLock::new(PipelineStats::default())),
        }
    }

    /// Add a transformation stage
    pub fn add_stage(mut self, stage: Box<dyn TransformStage>) -> Self {
        self.stages.push(stage);
        self
    }

    /// Process an event through the pipeline
    pub async fn process_event(&self, event: StreamEvent) -> Result<Option<StreamEvent>, StreamError> {
        let start_time = std::time::Instant::now();
        let mut current_event = Some(event);
        
        // Update processed count
        {
            let mut stats = self.stats.write().unwrap();
            stats.events_processed += 1;
        }

        // Process through each stage
        for stage in &self.stages {
            if let Some(event) = current_event {
                match stage.transform(event) {
                    Ok(result) => {
                        if !result.continue_pipeline {
                            current_event = None;
                            break;
                        }
                        current_event = result.event;
                        
                        // Update stage success count
                        let stage_name = stage.get_name().to_string();
                        // Note: In a real implementation, you'd track per-stage stats here
                    }
                    Err(e) => {
                        // Handle transformation error
                        if self.config.enable_error_recovery {
                            eprintln!("Stage '{}' failed: {}", stage.get_name(), e);
                            current_event = None;
                            break;
                        } else {
                            return Err(e);
                        }
                    }
                }
            } else {
                break;
            }
        }

        // Update statistics
        let processing_time = start_time.elapsed().as_micros() as f64;
        {
            let mut stats = self.stats.write().unwrap();
            stats.last_processed = Some(SystemTime::now());
            
            if current_event.is_some() {
                stats.events_transformed += 1;
            } else {
                stats.events_filtered += 1;
            }
            
            // Update average processing time
            let total_events = stats.events_processed as f64;
            stats.avg_processing_time_us = 
                (stats.avg_processing_time_us * (total_events - 1.0) + processing_time) / total_events;
        }

        Ok(current_event)
    }

    /// Get pipeline statistics
    pub fn get_stats(&self) -> PipelineStats {
        self.stats.read().unwrap().clone()
    }
}

/// Filter stage for event filtering
pub struct FilterStage {
    name: String,
    rules: Vec<FilterRule>,
    stats: Arc<RwLock<StageStats>>,
}

/// Filter rule definition
#[derive(Debug, Clone)]
pub struct FilterRule {
    pub rule_id: String,
    pub field: String,
    pub operator: FilterOperator,
    pub value: serde_json::Value,
    pub action: FilterAction,
}

/// Filter actions
#[derive(Debug, Clone)]
pub enum FilterAction {
    Keep,
    Drop,
    Modify(HashMap<String, serde_json::Value>),
}

impl FilterStage {
    /// Create new filter stage
    pub fn new(name: String, rules: Vec<FilterRule>) -> Self {
        Self {
            name,
            rules,
            stats: Arc::new(RwLock::new(StageStats::default())),
        }
    }
}

impl TransformStage for FilterStage {
    fn transform(&self, event: StreamEvent) -> Result<TransformResult, StreamError> {
        let start_time = std::time::Instant::now();
        let mut continue_pipeline = true;
        let mut transformed_event = event.clone();

        // Apply filter rules
        for rule in &self.rules {
            if self.matches_rule(&transformed_event, rule) {
                match &rule.action {
                    FilterAction::Drop => {
                        continue_pipeline = false;
                        break;
                    }
                    FilterAction::Keep => {
                        // Continue processing
                    }
                    FilterAction::Modify(modifications) => {
                        // Apply modifications to event
                        self.apply_modifications(&mut transformed_event, modifications);
                    }
                }
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.events_processed += 1;
            if continue_pipeline {
                stats.events_successful += 1;
            } else {
                stats.events_failed += 1;
            }
            stats.total_processing_time_us += start_time.elapsed().as_micros() as u64;
        }

        Ok(TransformResult {
            event: Some(transformed_event),
            continue_pipeline,
            metadata: HashMap::from([
                ("stage".to_string(), self.name.clone()),
                ("filtered".to_string(), (!continue_pipeline).to_string()),
            ]),
        })
    }

    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_stats(&self) -> StageStats {
        self.stats.read().unwrap().clone()
    }
}

impl FilterStage {
    /// Check if event matches filter rule
    fn matches_rule(&self, event: &StreamEvent, rule: &FilterRule) -> bool {
        // Extract field value from event
        let field_value = self.extract_field_value(event, &rule.field);
        
        match &rule.operator {
            FilterOperator::Equals => {
                field_value == Some(rule.value.clone())
            }
            FilterOperator::NotEquals => {
                field_value != Some(rule.value.clone())
            }
            FilterOperator::Contains => {
                if let Some(value) = field_value {
                    value.to_string().contains(&rule.value.to_string())
                } else {
                    false
                }
            }
            FilterOperator::Exists => {
                field_value.is_some()
            }
            FilterOperator::NotExists => {
                field_value.is_none()
            }
            _ => false, // Simplified for this example
        }
    }

    /// Extract field value from event
    fn extract_field_value(&self, event: &StreamEvent, field_path: &str) -> Option<serde_json::Value> {
        match field_path {
            "source" => Some(serde_json::Value::String(event.source.clone())),
            "priority" => Some(serde_json::Value::Number(event.priority.into())),
            "event_id" => Some(serde_json::Value::String(event.event_id.clone())),
            _ => {
                // Check metadata
                if let Some(prop_value) = event.metadata.get(field_path) {
                    Some(self.property_value_to_json(prop_value))
                } else {
                    None
                }
            }
        }
    }

    /// Convert PropertyValue to JSON
    fn property_value_to_json(&self, value: &PropertyValue) -> serde_json::Value {
        match value {
            PropertyValue::String(s) => serde_json::Value::String(s.clone()),
            PropertyValue::Int64(i) => serde_json::Value::Number((*i).into()),
            PropertyValue::Float(f) => serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0))),
            PropertyValue::Boolean(b) => serde_json::Value::Bool(*b),
            // Add other types as needed
            _ => serde_json::Value::Null,
        }
    }

    /// Apply modifications to event
    fn apply_modifications(&self, event: &mut StreamEvent, modifications: &HashMap<String, serde_json::Value>) {
        for (key, value) in modifications {
            if key == "priority" {
                if let Some(num) = value.as_u64() {
                    event.priority = num as u8;
                }
            } else {
                // Add to metadata
                let prop_value = self.json_to_property_value(value);
                event.metadata.insert(key.clone(), prop_value);
            }
        }
    }

    /// Convert JSON to PropertyValue
    fn json_to_property_value(&self, value: &serde_json::Value) -> PropertyValue {
        match value {
            serde_json::Value::String(s) => PropertyValue::string(s.clone()),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    PropertyValue::int64(i)
                } else if let Some(f) = n.as_f64() {
                    PropertyValue::float(f)
                } else {
                    PropertyValue::string(n.to_string())
                }
            }
            serde_json::Value::Bool(b) => PropertyValue::bool(*b),
            _ => PropertyValue::string(value.to_string()),
        }
    }
}

/// Enrichment stage for adding additional data
pub struct EnrichmentStage {
    name: String,
    enrichers: Vec<Box<dyn EventEnricher>>,
    stats: Arc<RwLock<StageStats>>,
}

/// Event enricher trait
pub trait EventEnricher: Send + Sync {
    /// Enrich an event
    fn enrich(&self, event: &mut StreamEvent) -> Result<(), StreamError>;
    
    /// Get enricher name
    fn get_name(&self) -> &str;
}

impl EnrichmentStage {
    /// Create new enrichment stage
    pub fn new(name: String, enrichers: Vec<Box<dyn EventEnricher>>) -> Self {
        Self {
            name,
            enrichers,
            stats: Arc::new(RwLock::new(StageStats::default())),
        }
    }
}

impl TransformStage for EnrichmentStage {
    fn transform(&self, mut event: StreamEvent) -> Result<TransformResult, StreamError> {
        let start_time = std::time::Instant::now();
        let mut enriched = false;

        // Apply all enrichers
        for enricher in &self.enrichers {
            match enricher.enrich(&mut event) {
                Ok(()) => {
                    enriched = true;
                }
                Err(e) => {
                    eprintln!("Enricher '{}' failed: {}", enricher.get_name(), e);
                    if !self.config.enable_error_recovery {
                        return Err(e);
                    }
                }
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.events_processed += 1;
            if enriched {
                stats.events_successful += 1;
            } else {
                stats.events_failed += 1;
            }
            stats.total_processing_time_us += start_time.elapsed().as_micros() as u64;
        }

        Ok(TransformResult {
            event: Some(event),
            continue_pipeline: true,
            metadata: HashMap::from([
                ("stage".to_string(), self.name.clone()),
                ("enriched".to_string(), enriched.to_string()),
            ]),
        })
    }

    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_stats(&self) -> StageStats {
        self.stats.read().unwrap().clone()
    }
}

// Add missing field
impl EnrichmentStage {
    fn config(&self) -> &PipelineConfig {
        // This would typically be stored as a field
        &PipelineConfig::default()
    }
}

/// Timestamp enricher
pub struct TimestampEnricher {
    name: String,
}

impl TimestampEnricher {
    pub fn new() -> Self {
        Self {
            name: "timestamp_enricher".to_string(),
        }
    }
}

impl EventEnricher for TimestampEnricher {
    fn enrich(&self, event: &mut StreamEvent) -> Result<(), StreamError> {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        
        event.metadata.insert(
            "processed_timestamp_ms".to_string(),
            PropertyValue::int64(current_time.as_millis() as i64),
        );
        
        event.metadata.insert(
            "processing_latency_ms".to_string(),
            PropertyValue::int64(
                current_time.saturating_sub(event.timestamp.duration_since(UNIX_EPOCH).unwrap())
                    .as_millis() as i64
            ),
        );

        Ok(())
    }

    fn get_name(&self) -> &str {
        &self.name
    }
}

/// Geolocation enricher
pub struct GeolocationEnricher {
    name: String,
    ip_geo_cache: Arc<RwLock<HashMap<String, GeolocationData>>>,
}

#[derive(Debug, Clone)]
pub struct GeolocationData {
    pub country: String,
    pub region: String,
    pub city: String,
    pub latitude: f64,
    pub longitude: f64,
}

impl GeolocationEnricher {
    pub fn new() -> Self {
        Self {
            name: "geolocation_enricher".to_string(),
            ip_geo_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Mock geolocation lookup (in reality, this would call a geolocation service)
    fn lookup_ip(&self, ip: &str) -> GeolocationData {
        // Mock data based on IP patterns
        if ip.starts_with("192.168.") {
            GeolocationData {
                country: "Local".to_string(),
                region: "Internal".to_string(),
                city: "Office".to_string(),
                latitude: 37.7749,
                longitude: -122.4194,
            }
        } else if ip.starts_with("10.") {
            GeolocationData {
                country: "Local".to_string(),
                region: "Private".to_string(),
                city: "Datacenter".to_string(),
                latitude: 40.7128,
                longitude: -74.0060,
            }
        } else {
            GeolocationData {
                country: "Unknown".to_string(),
                region: "Unknown".to_string(),
                city: "Unknown".to_string(),
                latitude: 0.0,
                longitude: 0.0,
            }
        }
    }
}

impl EventEnricher for GeolocationEnricher {
    fn enrich(&self, event: &mut StreamEvent) -> Result<(), StreamError> {
        // Look for IP address in event data or metadata
        if let Some(ip_prop) = event.metadata.get("ip_address") {
            if let Some(ip_str) = ip_prop.as_string() {
                let geo_data = {
                    let mut cache = self.ip_geo_cache.write().unwrap();
                    if !cache.contains_key(ip_str) {
                        cache.insert(ip_str.clone(), self.lookup_ip(ip_str));
                    }
                    cache.get(ip_str).cloned().unwrap()
                };

                event.metadata.insert(
                    "geo_country".to_string(),
                    PropertyValue::string(geo_data.country),
                );
                event.metadata.insert(
                    "geo_region".to_string(),
                    PropertyValue::string(geo_data.region),
                );
                event.metadata.insert(
                    "geo_city".to_string(),
                    PropertyValue::string(geo_data.city),
                );
                event.metadata.insert(
                    "geo_latitude".to_string(),
                    PropertyValue::float(geo_data.latitude),
                );
                event.metadata.insert(
                    "geo_longitude".to_string(),
                    PropertyValue::float(geo_data.longitude),
                );
            }
        }

        Ok(())
    }

    fn get_name(&self) -> &str {
        &self.name
    }
}