pub mod error;
pub mod types;
pub mod value;
pub mod table;

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

pub use error::StorageError;
pub use types::DataType;
pub use value::Value;
pub use table::{Table, Column, Row};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), StorageError> {
        if self.tables.contains_key(&name) {
            return Err(StorageError::DuplicateKey(name));
        }
        self.tables.insert(name.clone(), Table::new(name, columns));
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        self.tables
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| StorageError::TableNotFound(name.to_string()))
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    pub fn table_names(&self) -> Vec<&String> {
        self.tables.keys().collect()
    }
}
