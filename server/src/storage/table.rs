use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use super::error::StorageError;
use super::types::DataType;
use super::value::Value;
use super::search::SearchIndex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub id: String,
    pub values: Vec<Value>,
}

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub indexes: HashMap<String, BTreeMap<Value, Vec<String>>>, // column_name -> { value -> [row_ids] }
    pub search_index: Option<Arc<Mutex<SearchIndex>>>,
}

#[derive(Serialize, Deserialize)]
struct TableSerde {
    name: String,
    columns: Vec<Column>,
    rows: Vec<Row>,
    indexes: HashMap<String, BTreeMap<Value, Vec<String>>>,
}

impl From<TableSerde> for Table {
    fn from(s: TableSerde) -> Self {
        Self {
            name: s.name,
            columns: s.columns,
            rows: s.rows,
            indexes: s.indexes,
            search_index: None,
        }
    }
}

impl From<&Table> for TableSerde {
    fn from(t: &Table) -> Self {
        Self {
            name: t.name.clone(),
            columns: t.columns.clone(),
            rows: t.rows.clone(),
            indexes: t.indexes.clone(),
        }
    }
}

// Custom Serialize for Table
impl Serialize for Table {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        TableSerde::from(self).serialize(serializer)
    }
}

// Custom Deserialize for Table
impl<'de> Deserialize<'de> for Table {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        TableSerde::deserialize(deserializer).map(Table::from)
    }
}

impl Clone for Table {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            columns: self.columns.clone(),
            rows: self.rows.clone(),
            indexes: self.indexes.clone(),
            search_index: self.search_index.clone(),
        }
    }
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("columns", &self.columns)
            .field("rows", &self.rows)
            .field("indexes", &self.indexes)
            .finish()
    }
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            rows: Vec::new(),
            indexes: HashMap::new(),
            search_index: None,
        }
    }

    pub fn create_index(&mut self, column_name: &str) -> Result<(), StorageError> {
        let col_idx = self.column_index(column_name)
            .ok_or_else(|| StorageError::ColumnNotFound(column_name.to_string()))?;

        let mut index = BTreeMap::new();
        for row in &self.rows {
            let val = row.values.get(col_idx).cloned().unwrap_or(Value::Null);
            index.entry(val).or_insert_with(Vec::new).push(row.id.clone());
        }

        self.indexes.insert(column_name.to_string(), index);
        Ok(())
    }

    pub fn setup_search_index(&mut self, path: &str) -> anyhow::Result<()> {
        let text_fields: Vec<String> = self.columns.iter()
            .filter(|c| c.data_type == DataType::Text || c.data_type == DataType::VarChar)
            .map(|c| c.name.clone())
            .collect();
        
        if !text_fields.is_empty() {
            let index = SearchIndex::new(path, &text_fields)?;
            self.search_index = Some(Arc::new(Mutex::new(index)));
            
            // Populate existing data
            let rows_to_index = self.rows.clone();
            for row in rows_to_index {
                self.index_row(&row)?;
            }
        }
        Ok(())
    }

    fn index_row(&self, row: &Row) -> anyhow::Result<()> {
        if let Some(ref search_index) = self.search_index {
            let mut field_values = Vec::new();
            for (i, col) in self.columns.iter().enumerate() {
                if col.data_type == DataType::Text || col.data_type == DataType::VarChar {
                    if let Some(val) = row.values.get(i).and_then(|v| v.as_text()) {
                        field_values.push((col.name.clone(), val.to_string()));
                    }
                }
            }
            search_index.lock().unwrap().add_document(&row.id, &field_values)?;
        }
        Ok(())
    }

    pub fn insert(&mut self, values: Vec<Value>) -> Result<String, StorageError> {
        if values.len() != self.columns.len() {
            return Err(StorageError::InvalidType(format!(
                "Expected {} columns, got {}",
                self.columns.len(),
                values.len()
            )));
        }

        let id = Uuid::new_v4().to_string();
        
        // Update indexes
        let mut index_updates = Vec::new();
        for col_name in self.indexes.keys() {
            if let Some(col_idx) = self.column_index(col_name) {
                let val = values.get(col_idx).cloned().unwrap_or(Value::Null);
                index_updates.push((col_name.clone(), val));
            }
        }

        for (col_name, val) in index_updates {
            if let Some(index) = self.indexes.get_mut(&col_name) {
                index.entry(val).or_insert_with(Vec::new).push(id.clone());
            }
        }

        let row = Row {
            id: id.clone(),
            values,
        };
        
        // Update Search Index
        if let Err(e) = self.index_row(&row) {
            return Err(StorageError::PersistenceError(format!("Search index error: {}", e)));
        }

        self.rows.push(row);
        Ok(id)
    }

    pub fn update(&mut self, id: &str, values: Vec<Value>) -> Result<(), StorageError> {
        if values.len() != self.columns.len() {
            return Err(StorageError::InvalidType(format!(
                "Expected {} columns, got {}",
                self.columns.len(),
                values.len()
            )));
        }

        if let Some(pos) = self.rows.iter().position(|r| r.id == id) {
            let old_values = self.rows[pos].values.clone();
            
            let mut index_updates = Vec::new();
            for col_name in self.indexes.keys() {
                if let Some(col_idx) = self.column_index(col_name) {
                    let old_val = old_values.get(col_idx).cloned().unwrap_or(Value::Null);
                    let new_val = values.get(col_idx).cloned().unwrap_or(Value::Null);
                    index_updates.push((col_name.clone(), old_val, new_val));
                }
            }

            for (col_name, old_val, new_val) in index_updates {
                if let Some(index) = self.indexes.get_mut(&col_name) {
                    if let Some(ids) = index.get_mut(&old_val) {
                        ids.retain(|row_id| row_id != id);
                    }
                    index.entry(new_val).or_insert_with(Vec::new).push(id.to_string());
                }
            }

            self.rows[pos].values = values;
            
            // Update Search Index
            let updated_row = self.rows[pos].clone();
            if let Err(e) = self.index_row(&updated_row) {
                return Err(StorageError::PersistenceError(format!("Search index error: {}", e)));
            }

            Ok(())
        } else {
            Err(StorageError::RowNotFound(id.to_string()))
        }
    }

    pub fn delete(&mut self, id: &str) -> Result<(), StorageError> {
        if let Some(pos) = self.rows.iter().position(|r| r.id == id) {
            let old_values = self.rows[pos].values.clone();

            let mut index_updates = Vec::new();
            for col_name in self.indexes.keys() {
                if let Some(col_idx) = self.column_index(col_name) {
                    let old_val = old_values.get(col_idx).cloned().unwrap_or(Value::Null);
                    index_updates.push((col_name.clone(), old_val));
                }
            }

            for (col_name, old_val) in index_updates {
                if let Some(index) = self.indexes.get_mut(&col_name) {
                    if let Some(ids) = index.get_mut(&old_val) {
                        ids.retain(|row_id| row_id != id);
                    }
                }
            }

            // Remove from Search Index
            if let Some(ref search_index) = self.search_index {
                if let Err(e) = search_index.lock().unwrap().delete_document(id) {
                    return Err(StorageError::PersistenceError(format!("Search index error: {}", e)));
                }
            }

            self.rows.remove(pos);
            Ok(())
        } else {
            Err(StorageError::RowNotFound(id.to_string()))
        }
    }

    pub fn get(&self, id: &str) -> Option<&Row> {
        self.rows.iter().find(|r| r.id == id)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn null_row(&self) -> Row {
        Row {
            id: "null".to_string(),
            values: vec![Value::Null; self.columns.len()],
        }
    }
}
