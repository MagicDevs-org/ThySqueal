pub mod error;
pub mod persistence;
pub mod table;
pub mod types;
pub mod value;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub use error::StorageError;
use persistence::Persister;
pub use table::{Column, Row, Table};
pub use types::DataType;
pub use value::Value;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatabaseState {
    pub tables: HashMap<String, Table>,
}

pub struct Database {
    state: DatabaseState,
    persister: Option<Box<dyn Persister>>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            state: DatabaseState::default(),
            persister: None,
        }
    }

    pub fn with_persister(persister: Box<dyn Persister>) -> Result<Self, StorageError> {
        let tables = persister.load_tables().unwrap_or_default();

        Ok(Self {
            state: DatabaseState { tables },
            persister: Some(persister),
        })
    }
    pub fn save(&self) -> Result<(), StorageError> {
        if let Some(persister) = &self.persister {
            persister.save_tables(&self.state.tables)
        } else {
            Ok(())
        }
    }

    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), StorageError> {
        if self.state.tables.contains_key(&name) {
            return Err(StorageError::DuplicateKey(name));
        }
        self.state
            .tables
            .insert(name.clone(), Table::new(name, columns));
        self.save()
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        self.state
            .tables
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| StorageError::TableNotFound(name.to_string()))?;
        self.save()
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.state.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.state.tables.get_mut(name)
    }

    pub fn table_names(&self) -> Vec<&String> {
        self.state.tables.keys().collect()
    }
}
