pub mod error;
pub mod types;
pub mod value;
pub mod table;
pub mod persistence;
pub mod search;

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

pub use error::StorageError;
pub use types::DataType;
pub use value::Value;
pub use table::{Table, Column, Row, TableIndex};
use persistence::Persister;
use crate::sql::executor::Executor;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatabaseState {
    pub tables: HashMap<String, Table>,
}

impl DatabaseState {
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

pub struct Database {
    state: DatabaseState,
    persister: Option<Box<dyn Persister>>,
    _data_dir: Option<String>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            state: DatabaseState::default(),
            persister: None,
            _data_dir: None,
        }
    }

    pub fn with_persister(persister: Box<dyn Persister>, data_dir: String) -> Result<Self, StorageError> {
        let mut tables = persister.load_tables().unwrap_or_default();

        // Initialize search indices for each table
        for (name, table) in &mut tables {
            let search_path = format!("{}/search_{}", data_dir, name);
            table.setup_search_index(&search_path).map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        }

        Ok(Self {
            state: DatabaseState { tables },
            persister: Some(persister),
            _data_dir: Some(data_dir),
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
        
        let mut table = Table::new(name.clone(), columns);
        
        // Initialize search index if data_dir is available
        if let Some(ref data_dir) = self._data_dir {
            let search_path = format!("{}/search_{}", data_dir, name);
            table.setup_search_index(&search_path).map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        }

        self.state.tables.insert(name, table);
        self.save()
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        self.state.tables
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| StorageError::TableNotFound(name.to_string()))?;
        self.save()
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.state.get_table(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.state.get_table_mut(name)
    }

    #[allow(dead_code)]
    pub fn table_names(&self) -> Vec<&String> {
        self.state.table_names()
    }

    pub fn state(&self) -> &DatabaseState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut DatabaseState {
        &mut self.state
    }

    pub fn set_state(&mut self, state: DatabaseState) -> Result<(), StorageError> {
        self.state = state;
        self.save()
    }

    // Methods that need Executor for index evaluation
    pub fn insert(&mut self, executor: &Executor, table_name: &str, values: Vec<Value>, db_state: &DatabaseState) -> Result<String, StorageError> {
        let table = self.get_table_mut(table_name).ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        let id = table.insert(executor, values, db_state)?;
        self.save()?;
        Ok(id)
    }

    pub fn _update(&mut self, executor: &Executor, table_name: &str, id: &str, values: Vec<Value>, db_state: &DatabaseState) -> Result<(), StorageError> {
        let table = self.get_table_mut(table_name).ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        table.update(executor, id, values, db_state)?;
        self.save()
    }

    pub fn _delete(&mut self, executor: &Executor, table_name: &str, id: &str, db_state: &DatabaseState) -> Result<(), StorageError> {
        let table = self.get_table_mut(table_name).ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        table.delete(executor, id, db_state)?;
        self.save()
    }
}
