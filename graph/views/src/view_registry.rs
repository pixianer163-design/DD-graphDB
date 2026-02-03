//! View Registry and Storage Management
//!
//! This module provides persistent storage for materialized view definitions
//! and their computed results. Views are stored separately from graph data.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use bincode;
use chrono::{DateTime, Utc};

use crate::{
    ViewType, RefreshPolicy, ViewSizeMetrics, ViewError,
    ViewCacheData
};

/// View definition with extended metadata for SQL-style views
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDefinition {
    /// Unique view identifier
    pub name: String,
    /// View type and configuration
    pub view_type: ViewType,
    /// Refresh policy
    pub refresh_policy: RefreshPolicy,
    /// Size metrics
    pub size_metrics: ViewSizeMetrics,
    /// Original DSL definition (SQL or other)
    pub dsl_definition: Option<String>,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Last refreshed timestamp
    pub last_refreshed: DateTime<Utc>,
    /// View dependencies (other views this depends on)
    pub dependencies: Vec<String>,
    /// Storage format and location
    pub storage_info: ViewStorageInfo,
}

/// Storage information for view data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ViewStorageInfo {
    /// In-memory only (not persisted)
    Memory,
    /// Persisted to disk with given path
    Disk { path: PathBuf, format: StorageFormat },
    /// Distributed storage
    Distributed { nodes: Vec<String> },
}

/// Storage format for view data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageFormat {
    /// JSON format (human-readable)
    Json,
    /// Bincode format (compact, fast)
    Bincode,
    /// Columnar format for analytics
    Columnar,
    /// Delta format for incremental updates
    Delta,
}

/// View registry manages all view definitions
pub struct ViewRegistry {
    /// Storage directory for view definitions
    storage_dir: PathBuf,
    /// In-memory cache of view definitions
    definitions: Arc<RwLock<HashMap<String, ViewDefinition>>>,
    /// View data storage
    view_store: ViewStore,
}

impl ViewRegistry {
    /// Create a new view registry with given storage directory
    pub fn new(storage_dir: impl AsRef<Path>) -> Result<Self, ViewError> {
        let storage_dir = storage_dir.as_ref().to_path_buf();
        
        // Create directory if it doesn't exist
        if !storage_dir.exists() {
            std::fs::create_dir_all(&storage_dir)
                .map_err(|e| ViewError::StorageError {
                    error: format!("Failed to create view storage directory: {}", e),
                })?;
        }
        
        let definitions = Arc::new(RwLock::new(HashMap::new()));
        let view_store = ViewStore::new(storage_dir.join("data"))?;
        
        Ok(Self {
            storage_dir,
            definitions,
            view_store,
        })
    }
    
    /// Register a new view definition
    pub fn register_view(&self, definition: ViewDefinition) -> Result<(), ViewError> {
        let name = definition.name.clone();
        
        // Check if view already exists
        {
            let definitions = self.definitions.read()
                .map_err(|e| ViewError::StorageError {
                    error: format!("Failed to read view definitions: {}", e),
                })?;
            
            if definitions.contains_key(&name) {
                return Err(ViewError::InvalidDefinition {
                    details: format!("View '{}' already exists", name),
                });
            }
        }
        
        // Save definition to disk
        self.save_definition(&definition)?;
        
        // Update in-memory cache
        {
            let mut definitions = self.definitions.write()
                .map_err(|e| ViewError::StorageError {
                    error: format!("Failed to write view definitions: {}", e),
                })?;
            
            definitions.insert(name, definition);
        }
        
        Ok(())
    }
    
    /// Get a view definition by name
    pub fn get_view(&self, name: &str) -> Result<Option<ViewDefinition>, ViewError> {
        let definitions = self.definitions.read()
            .map_err(|e| ViewError::StorageError {
                error: format!("Failed to read view definitions: {}", e),
            })?;
        
        Ok(definitions.get(name).cloned())
    }
    
    /// List all registered views
    pub fn list_views(&self) -> Result<Vec<ViewDefinition>, ViewError> {
        let definitions = self.definitions.read()
            .map_err(|e| ViewError::StorageError {
                error: format!("Failed to read view definitions: {}", e),
            })?;
        
        Ok(definitions.values().cloned().collect())
    }
    
    /// Delete a view definition and its data
    pub fn delete_view(&self, name: &str) -> Result<(), ViewError> {
        // Remove from memory
        {
            let mut definitions = self.definitions.write()
                .map_err(|e| ViewError::StorageError {
                    error: format!("Failed to write view definitions: {}", e),
                })?;
            
            definitions.remove(name);
        }
        
        // Remove from disk
        let definition_path = self.storage_dir.join("definitions").join(format!("{}.json", name));
        if definition_path.exists() {
            std::fs::remove_file(definition_path)
                .map_err(|e| ViewError::StorageError {
                    error: format!("Failed to delete view definition: {}", e),
                })?;
        }
        
        // Remove view data
        self.view_store.delete_view_data(name)?;
        
        Ok(())
    }
    
    /// Save view definition to disk
    fn save_definition(&self, definition: &ViewDefinition) -> Result<(), ViewError> {
        let definitions_dir = self.storage_dir.join("definitions");
        if !definitions_dir.exists() {
            std::fs::create_dir_all(&definitions_dir)
                .map_err(|e| ViewError::StorageError {
                    error: format!("Failed to create definitions directory: {}", e),
                })?;
        }
        
        let definition_path = definitions_dir.join(format!("{}.json", definition.name));
        let json = serde_json::to_string_pretty(definition)
            .map_err(|e| ViewError::StorageError {
                error: format!("Failed to serialize view definition: {}", e),
            })?;
        
        std::fs::write(definition_path, json)
            .map_err(|e| ViewError::StorageError {
                error: format!("Failed to write view definition: {}", e),
            })?;
        
        Ok(())
    }
    
    /// Load all view definitions from disk
    pub fn load_from_disk(&self) -> Result<(), ViewError> {
        let definitions_dir = self.storage_dir.join("definitions");
        if !definitions_dir.exists() {
            return Ok(());
        }
        
        let mut definitions = HashMap::new();
        
        for entry in std::fs::read_dir(definitions_dir)
            .map_err(|e| ViewError::StorageError {
                error: format!("Failed to read definitions directory: {}", e),
            })? {
            let entry = entry.map_err(|e| ViewError::StorageError {
                error: format!("Failed to read directory entry: {}", e),
            })?;
            
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
                let json = std::fs::read_to_string(&path)
                    .map_err(|e| ViewError::StorageError {
                        error: format!("Failed to read view definition file: {}", e),
                    })?;
                
                let definition: ViewDefinition = serde_json::from_str(&json)
                    .map_err(|e| ViewError::StorageError {
                        error: format!("Failed to parse view definition: {}", e),
                    })?;
                
                definitions.insert(definition.name.clone(), definition);
            }
        }
        
        let mut current_definitions = self.definitions.write()
            .map_err(|e| ViewError::StorageError {
                error: format!("Failed to write view definitions: {}", e),
            })?;
        
        *current_definitions = definitions;
        
        Ok(())
    }
    
    /// Get view store for data operations
    pub fn view_store(&self) -> &ViewStore {
        &self.view_store
    }
}

/// View store manages materialized view data
pub struct ViewStore {
    /// Storage directory for view data
    storage_dir: PathBuf,
}

impl ViewStore {
    /// Create a new view store
    pub fn new(storage_dir: impl AsRef<Path>) -> Result<Self, ViewError> {
        let storage_dir = storage_dir.as_ref().to_path_buf();
        
        if !storage_dir.exists() {
            std::fs::create_dir_all(&storage_dir)
                .map_err(|e| ViewError::StorageError {
                    error: format!("Failed to create view data directory: {}", e),
                })?;
        }
        
        Ok(Self { storage_dir })
    }
    
    /// Save view data
    pub fn save_view_data(&self, view_name: &str, data: &ViewCacheData) -> Result<(), ViewError> {
        let view_dir = self.storage_dir.join(view_name);
        if !view_dir.exists() {
            std::fs::create_dir_all(&view_dir)
                .map_err(|e| ViewError::StorageError {
                    error: format!("Failed to create view directory: {}", e),
                })?;
        }
        
        let data_path = view_dir.join("data.bin");
        let bytes = bincode::serialize(data)
            .map_err(|e| ViewError::StorageError {
                error: format!("Failed to serialize view data: {}", e),
            })?;
        
        std::fs::write(data_path, bytes)
            .map_err(|e| ViewError::StorageError {
                error: format!("Failed to write view data: {}", e),
            })?;
        
        Ok(())
    }
    
    /// Load view data
    pub fn load_view_data(&self, view_name: &str) -> Result<Option<ViewCacheData>, ViewError> {
        let data_path = self.storage_dir.join(view_name).join("data.bin");
        if !data_path.exists() {
            return Ok(None);
        }
        
        let bytes = std::fs::read(data_path)
            .map_err(|e| ViewError::StorageError {
                error: format!("Failed to read view data: {}", e),
            })?;
        
        let data = bincode::deserialize(&bytes)
            .map_err(|e| ViewError::StorageError {
                error: format!("Failed to deserialize view data: {}", e),
            })?;
        
        Ok(Some(data))
    }
    
    /// Delete view data
    pub fn delete_view_data(&self, view_name: &str) -> Result<(), ViewError> {
        let view_dir = self.storage_dir.join(view_name);
        if view_dir.exists() {
            std::fs::remove_dir_all(view_dir)
                .map_err(|e| ViewError::StorageError {
                    error: format!("Failed to delete view data directory: {}", e),
                })?;
        }
        
        Ok(())
    }
    
    /// Check if view data exists
    pub fn has_view_data(&self, view_name: &str) -> bool {
        self.storage_dir.join(view_name).join("data.bin").exists()
    }
}