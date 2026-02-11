//! Real-time Windowed Operations and Aggregations
//!
//! This module implements time-based windowed operations and real-time
//! aggregations for stream processing scenarios.

use std::collections::{HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use graph_core::{PropertyValue};

use super::{
    StreamEvent, StreamEventData
};

/// Window definition for time-based operations
#[derive(Debug, Clone)]
pub struct WindowSpec {
    /// Window identifier
    pub window_id: String,
    /// Window type
    pub window_type: WindowType,
    /// Window duration
    pub duration: Duration,
    /// Slide interval (for sliding windows)
    pub slide_interval: Duration,
    /// Maximum number of events per window
    pub max_events: usize,
    /// Window metadata
    pub metadata: HashMap<String, PropertyValue>,
}

/// Types of windows
#[derive(Debug, Clone, PartialEq)]
pub enum WindowType {
    /// Tumbling window (fixed, non-overlapping)
    Tumbling,
    /// Sliding window (overlapping)
    Sliding,
    /// Session window (based on session gaps)
    Session,
    /// Global window (all events so far)
    Global,
    /// Count-based window
    Count,
}

/// Window operation result
#[derive(Debug, Clone)]
pub struct WindowResult {
    /// Window identifier
    pub window_id: String,
    /// Window start time
    pub window_start: SystemTime,
    /// Window end time
    pub window_end: SystemTime,
    /// Number of events in window
    pub event_count: usize,
    /// Aggregated results
    pub aggregations: HashMap<String, PropertyValue>,
    /// Window metadata
    pub metadata: HashMap<String, PropertyValue>,
}

/// Window state for active windows
#[derive(Debug, Clone)]
pub struct WindowState {
    /// Window specification
    pub spec: WindowSpec,
    /// Current events in window
    pub events: Vec<StreamEvent>,
    /// Window start time
    pub start_time: Option<SystemTime>,
    /// Last event time in window
    pub last_event_time: Option<SystemTime>,
    /// Window is closed
    pub is_closed: bool,
    /// Event count
    pub event_count: usize,
}

/// Real-time aggregation functions
pub enum AggregationFunction {
    /// Count events
    Count,
    /// Sum numeric values
    Sum(String), // field name
    /// Average of numeric values
    Average(String), // field name
    /// Minimum value
    Min(String), // field name
    /// Maximum value
    Max(String), // field name
    /// Distinct count
    Distinct(String), // field name
    /// Custom aggregation (uses Arc for safe cloning)
    Custom(String, Arc<dyn Fn(&[StreamEvent]) -> PropertyValue + Send + Sync>),
}

impl std::fmt::Debug for AggregationFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count => write!(f, "Count"),
            Self::Sum(field) => write!(f, "Sum({})", field),
            Self::Average(field) => write!(f, "Average({})", field),
            Self::Min(field) => write!(f, "Min({})", field),
            Self::Max(field) => write!(f, "Max({})", field),
            Self::Distinct(field) => write!(f, "Distinct({})", field),
            Self::Custom(name, _) => write!(f, "Custom({})", name),
        }
    }
}

impl Clone for AggregationFunction {
    fn clone(&self) -> Self {
        match self {
            Self::Count => Self::Count,
            Self::Sum(field) => Self::Sum(field.clone()),
            Self::Average(field) => Self::Average(field.clone()),
            Self::Min(field) => Self::Min(field.clone()),
            Self::Max(field) => Self::Max(field.clone()),
            Self::Distinct(field) => Self::Distinct(field.clone()),
            Self::Custom(name, func) => Self::Custom(name.clone(), Arc::clone(func)),
        }
    }
}

/// Windowed operations manager
#[derive(Debug)]
pub struct WindowManager {
    /// Active windows
    windows: Arc<RwLock<HashMap<String, WindowState>>>,
    /// Window specifications
    window_specs: HashMap<String, WindowSpec>,
    /// Aggregation functions
    aggregation_functions: HashMap<String, AggregationFunction>,
    /// Processing statistics
    stats: Arc<RwLock<WindowStats>>,
}

/// Window processing statistics
#[derive(Debug, Clone, Default)]
pub struct WindowStats {
    /// Total windows processed
    pub windows_processed: u64,
    /// Total events processed
    pub events_processed: u64,
    /// Average processing time per window
    pub avg_processing_time_ms: f64,
    /// Max processing time per window
    pub max_processing_time_ms: u64,
    /// Active windows count
    pub active_windows: usize,
}

impl WindowManager {
    /// Create new window manager
    pub fn new() -> Self {
        Self {
            windows: Arc::new(RwLock::new(HashMap::new())),
            window_specs: HashMap::new(),
            aggregation_functions: HashMap::new(),
            stats: Arc::new(RwLock::new(WindowStats::default())),
        }
    }

    /// Register a window specification
    pub fn register_window(&mut self, spec: WindowSpec) -> Result<(), WindowError> {
        self.window_specs.insert(spec.window_id.clone(), spec.clone());
        
        // Initialize window state
        let state = WindowState {
            spec: spec.clone(),
            events: Vec::new(),
            start_time: None,
            last_event_time: None,
            is_closed: false,
            event_count: 0,
        };

        {
            let mut windows = self.windows.write().unwrap();
            windows.insert(spec.window_id.clone(), state);
        }

        println!("✅ Registered window: {} ({:?})", spec.window_id, spec.window_type);
        Ok(())
    }

    /// Register aggregation function for a window
    pub fn register_aggregation(&mut self, window_id: &str, function: AggregationFunction) -> Result<(), WindowError> {
        let function_clone = function.clone();
        self.aggregation_functions.insert(window_id.to_string(), function);
        println!("✅ Registered aggregation for window {}: {:?}", window_id, function_clone);
        Ok(())
    }

    /// Process events through all active windows
    pub fn process_event(&self, event: StreamEvent) -> Result<Vec<WindowResult>, WindowError> {
        let mut results = Vec::new();
        let processing_start = Instant::now();

        {
            let mut windows = self.windows.write().unwrap();
            
            // Process event through each active window
            for (window_id, state) in windows.iter_mut() {
                if state.is_closed {
                    continue;
                }

                // Add event to window
                state.events.push(event.clone());
                state.event_count += 1;
                state.last_event_time = Some(event.timestamp);

                // Initialize window start time if needed
                if state.start_time.is_none() {
                    state.start_time = Some(event.timestamp);
                }

                // Check if window should be closed
                if self.should_close_window(state) {
                    let window_result = self.close_window(window_id, state)?;
                    results.push(window_result);
                    
                    // Reset window state for next window
                    *state = WindowState {
                        spec: state.spec.clone(),
                        events: Vec::new(),
                        start_time: None,
                        last_event_time: None,
                        is_closed: false,
                        event_count: 0,
                    };
                }
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.events_processed += 1;
            
            let processing_time = processing_start.elapsed().as_millis() as f64;
            if stats.windows_processed > 0 {
                stats.avg_processing_time_ms = 
                    (stats.avg_processing_time_ms * (stats.windows_processed - 1) as f64 + processing_time) / stats.windows_processed as f64;
            } else {
                stats.avg_processing_time_ms = processing_time;
            }
            
            stats.max_processing_time_ms = stats.max_processing_time_ms.max(processing_time as u64);
        }

        Ok(results)
    }

    /// Process batch of events through windows
    pub fn process_batch(&self, events: Vec<StreamEvent>) -> Result<Vec<WindowResult>, WindowError> {
        let mut all_results = Vec::new();
        
        for event in events {
            let results = self.process_event(event)?;
            all_results.extend(results);
        }

        Ok(all_results)
    }

    /// Check if window should be closed based on its specification
    fn should_close_window(&self, state: &WindowState) -> bool {
        match &state.spec.window_type {
            WindowType::Tumbling => {
                // Close when duration is reached
                if let Some(start_time) = state.start_time {
                    let elapsed = start_time.elapsed().unwrap_or(Duration::from_secs(0));
                    elapsed >= state.spec.duration
                } else {
                    false
                }
            }
            WindowType::Sliding => {
                // Close when duration is reached
                if let Some(start_time) = state.start_time {
                    let elapsed = start_time.elapsed().unwrap_or(Duration::from_secs(0));
                    elapsed >= state.spec.duration
                } else {
                    false
                }
            }
            WindowType::Session => {
                // Close when gap exceeds threshold (default 30 seconds)
                if let (Some(last_time), Some(current_time)) = (state.last_event_time, state.events.last().map(|e| e.timestamp)) {
                    let gap = current_time.duration_since(last_time).unwrap_or(Duration::from_secs(0));
                    gap >= Duration::from_secs(30)
                } else {
                    false
                }
            }
            WindowType::Count => {
                // Close when max events reached
                state.event_count >= state.spec.max_events
            }
            WindowType::Global => {
                // Never close global window
                false
            }
        }
    }

    /// Close a window and compute aggregations
    fn close_window(&self, window_id: &str, state: &WindowState) -> Result<WindowResult, WindowError> {
        let window_end = SystemTime::now();
        let window_start = state.start_time.unwrap_or(window_end);

        // Compute aggregations
        let mut aggregations = HashMap::new();
        
        if let Some(function) = self.aggregation_functions.get(window_id) {
            aggregations = self.compute_aggregation(function, &state.events)?;
        } else {
            // Default aggregations
            aggregations.insert("count".to_string(), PropertyValue::int64(state.event_count as i64));
            aggregations.insert("duration_ms".to_string(), PropertyValue::float64(
                window_start.elapsed().unwrap_or(Duration::from_secs(0)).as_millis() as f64
            ));
        }

        let result = WindowResult {
            window_id: window_id.to_string(),
            window_start,
            window_end,
            event_count: state.event_count,
            aggregations,
            metadata: state.spec.metadata.clone(),
        };

        println!("📊 Window {} closed: {} events in {:?}ms", 
            window_id, state.event_count, 
            window_start.elapsed().unwrap_or(Duration::from_secs(0)).as_millis());

        Ok(result)
    }

    /// Compute aggregation for events
    fn compute_aggregation(&self, function: &AggregationFunction, events: &[StreamEvent]) -> Result<HashMap<String, PropertyValue>, WindowError> {
        let mut result = HashMap::new();

        match function {
            AggregationFunction::Count => {
                result.insert("count".to_string(), PropertyValue::int64(events.len() as i64));
            }
            AggregationFunction::Sum(field_name) => {
                let sum = self.extract_numeric_sum(events, field_name)?;
                result.insert("sum".to_string(), PropertyValue::float64(sum));
            }
            AggregationFunction::Average(field_name) => {
                let sum = self.extract_numeric_sum(events, field_name)?;
                let count = events.len() as f64;
                let avg = if count > 0.0 { sum / count } else { 0.0 };
                result.insert("average".to_string(), PropertyValue::float64(avg));
            }
            AggregationFunction::Min(field_name) => {
                let min_val = self.extract_numeric_min(events, field_name)?;
                result.insert("min".to_string(), PropertyValue::float64(min_val));
            }
            AggregationFunction::Max(field_name) => {
                let max_val = self.extract_numeric_max(events, field_name)?;
                result.insert("max".to_string(), PropertyValue::float64(max_val));
            }
            AggregationFunction::Distinct(field_name) => {
                let distinct_count = self.extract_distinct_count(events, field_name)?;
                result.insert("distinct".to_string(), PropertyValue::int64(distinct_count as i64));
            }
            AggregationFunction::Custom(_, func) => {
                let custom_result = func(events);
                result.insert("custom".to_string(), custom_result);
            }
        }

        Ok(result)
    }

    /// Extract numeric sum from events
    fn extract_numeric_sum(&self, events: &[StreamEvent], field_name: &str) -> Result<f64, WindowError> {
        let mut sum = 0.0;
        
        for event in events {
            if let Some(value) = self.extract_numeric_value(event, field_name)? {
                sum += value;
            }
        }

        Ok(sum)
    }

    /// Extract average numeric value from events
    #[allow(dead_code)]
    fn extract_numeric_avg(&self, events: &[StreamEvent], field_name: &str) -> Result<f64, WindowError> {
        let sum = self.extract_numeric_sum(events, field_name)?;
        let count = self.extract_numeric_count(events, field_name)?;
        
        if count > 0 {
            Ok(sum / count as f64)
        } else {
            Ok(0.0)
        }
    }

    /// Extract minimum numeric value from events
    fn extract_numeric_min(&self, events: &[StreamEvent], field_name: &str) -> Result<f64, WindowError> {
        let mut min_val = None;
        
        for event in events {
            if let Some(value) = self.extract_numeric_value(event, field_name)? {
                if let Some(current_min) = min_val {
                    if value < current_min {
                        min_val = Some(value);
                    }
                } else {
                    min_val = Some(value);
                }
            }
        }

        Ok(min_val.unwrap_or(0.0))
    }

    /// Extract maximum numeric value from events
    fn extract_numeric_max(&self, events: &[StreamEvent], field_name: &str) -> Result<f64, WindowError> {
        let mut max_val = None;
        
        for event in events {
            if let Some(value) = self.extract_numeric_value(event, field_name)? {
                if let Some(current_max) = max_val {
                    if value > current_max {
                        max_val = Some(value);
                    }
                } else {
                    max_val = Some(value);
                }
            }
        }

        Ok(max_val.unwrap_or(0.0))
    }

    /// Extract numeric count from events
    #[allow(dead_code)]
    fn extract_numeric_count(&self, events: &[StreamEvent], _field_name: &str) -> Result<usize, WindowError> {
        let mut count = 0;
        
        for event in events {
            if self.extract_numeric_value(event, "value").is_ok() {
                count += 1;
            }
        }

        Ok(count)
    }

    /// Extract distinct count from events
    fn extract_distinct_count(&self, events: &[StreamEvent], field_name: &str) -> Result<usize, WindowError> {
        let mut distinct_values = std::collections::HashSet::new();
        
        for event in events {
            if let Ok(Some(value)) = self.extract_string_value(event, field_name) {
                distinct_values.insert(value);
            }
        }

        Ok(distinct_values.len())
    }

    /// Extract numeric value from event
    fn extract_numeric_value(&self, event: &StreamEvent, field_name: &str) -> Result<Option<f64>, WindowError> {
        match &event.data {
            StreamEventData::Vertex(_, ref props) => {
                if let Some(value) = props.get(field_name) {
                    match value {
                        PropertyValue::Int64(i) => Ok(Some(*i as f64)),
                        PropertyValue::Float64(f) => Ok(Some(*f)),
                        _ => Err(WindowError::UnsupportedDataType {
                            field: field_name.to_string(),
                            data_type: "numeric".to_string(),
                        }),
                    }
                } else {
                    Ok(None)
                }
            }
            StreamEventData::Edge(_, ref props) => {
                if let Some(value) = props.get(field_name) {
                    match value {
                        PropertyValue::Int64(i) => Ok(Some(*i as f64)),
                        PropertyValue::Float64(f) => Ok(Some(*f)),
                        _ => Err(WindowError::UnsupportedDataType {
                            field: field_name.to_string(),
                            data_type: "numeric".to_string(),
                        }),
                    }
                } else {
                    Ok(None)
                }
            }
            StreamEventData::AnalyticsResult(_, _) => {
                // For analytics results, we need to parse the result
                Ok(None) // Simplified for now
            }
            StreamEventData::SystemMetrics(ref metrics) => {
                if let Some(value) = metrics.get(field_name) {
                    match value {
                        PropertyValue::Int64(i) => Ok(Some(*i as f64)),
                        PropertyValue::Float64(f) => Ok(Some(*f)),
                        _ => Err(WindowError::UnsupportedDataType {
                            field: field_name.to_string(),
                            data_type: "numeric".to_string(),
                        }),
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// Extract string value from event
    fn extract_string_value(&self, event: &StreamEvent, field_name: &str) -> Result<Option<String>, WindowError> {
        match &event.data {
            StreamEventData::Vertex(_, ref props) => {
                if let Some(value) = props.get(field_name) {
                    match value {
                        PropertyValue::String(s) => Ok(Some(s.clone())),
                        _ => Err(WindowError::UnsupportedDataType {
                            field: field_name.to_string(),
                            data_type: "string".to_string(),
                        }),
                    }
                } else {
                    Ok(None)
                }
            }
            StreamEventData::Edge(_, ref props) => {
                if let Some(value) = props.get(field_name) {
                    match value {
                        PropertyValue::String(s) => Ok(Some(s.clone())),
                        _ => Err(WindowError::UnsupportedDataType {
                            field: field_name.to_string(),
                            data_type: "string".to_string(),
                        }),
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// Get all active windows
    pub fn get_active_windows(&self) -> HashMap<String, WindowState> {
        self.windows.read().unwrap().clone()
    }

    /// Get window processing statistics
    pub fn get_stats(&self) -> WindowStats {
        let mut stats = self.stats.read().unwrap().clone();
        stats.active_windows = self.windows.read().unwrap().len();
        stats
    }

    /// Reset all windows
    pub fn reset_all(&self) {
        {
            let mut windows = self.windows.write().unwrap();
            windows.clear();
        }
        
        {
            let mut stats = self.stats.write().unwrap();
            *stats = WindowStats::default();
        }
    }
}

/// Window processing errors
#[derive(Debug, thiserror::Error)]
pub enum WindowError {
    #[error("Window not found: {window_id}")]
    WindowNotFound { window_id: String },
    
    #[error("Unsupported data type for field {field}: {data_type}")]
    UnsupportedDataType { field: String, data_type: String },
    
    #[error("Aggregation failed: {reason}")]
    AggregationFailed { reason: String },
    
    #[error("Window processing failed: {error}")]
    ProcessingFailed { error: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_spec_creation() {
        let spec = WindowSpec {
            window_id: "test_window".to_string(),
            window_type: WindowType::Tumbling,
            duration: Duration::from_secs(60),
            slide_interval: Duration::from_secs(0),
            max_events: 1000,
            metadata: HashMap::new(),
        };

        assert_eq!(spec.window_type, WindowType::Tumbling);
        assert_eq!(spec.max_events, 1000);
    }

    #[test]
    fn test_window_result_creation() {
        let result = WindowResult {
            window_id: "test_window".to_string(),
            window_start: SystemTime::now(),
            window_end: SystemTime::now(),
            event_count: 10,
            aggregations: HashMap::from([
                ("count".to_string(), PropertyValue::int64(10)),
            ]),
            metadata: HashMap::new(),
        };

        assert_eq!(result.event_count, 10);
        assert_eq!(result.aggregations.get("count"), Some(&PropertyValue::int64(10)));
    }

    #[test]
    fn test_aggregation_functions() {
        let count_fn = AggregationFunction::Count;
        assert!(matches!(count_fn, AggregationFunction::Count));

        let sum_fn = AggregationFunction::Sum("value".to_string());
        assert!(matches!(sum_fn, AggregationFunction::Sum(_)));

        let custom_fn = AggregationFunction::Custom("custom".to_string(), Arc::new(|_| PropertyValue::int64(42)));
        assert!(matches!(custom_fn, AggregationFunction::Custom(_, _)));
    }
}