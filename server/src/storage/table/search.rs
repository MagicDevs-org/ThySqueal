use super::super::row::Row;
use super::super::search::SearchIndex;
use super::Table;
use std::sync::{Arc, Mutex};

impl Table {
    pub fn enable_search_index(&mut self, data_dir: &str) {
        let path = format!("{}/search_{}", data_dir, self.schema.name);

        if std::path::Path::new(&path).exists() {
            let schema_file = format!("{}/schema_fields.json", path);
            let current_fields: std::collections::HashSet<_> =
                self.schema.columns.iter().map(|c| c.name.clone()).collect();
            if let Ok(content) = std::fs::read_to_string(&schema_file) {
                #[allow(clippy::collapsible_if)]
                if let Ok(stored) = serde_json::from_str::<serde_json::Value>(&content) {
                    if let Some(fields) = stored.get("fields").and_then(|f| f.as_array()) {
                        let stored_fields: std::collections::HashSet<_> = fields
                            .iter()
                            .filter_map(|f| f.as_str())
                            .map(|s| s.to_string())
                            .collect();
                        if stored_fields != current_fields {
                            let _ = std::fs::remove_dir_all(&path);
                        }
                    }
                }
            }
        }

        let fields: Vec<String> = self.schema.columns.iter().map(|c| c.name.clone()).collect();
        let mut index = match SearchIndex::new(&path, &fields) {
            Ok(idx) => idx,
            Err(e) => {
                eprintln!(
                    "Warning: Failed to create search index for {}: {}. Deleting and retrying.",
                    self.schema.name, e
                );
                let _ = std::fs::remove_dir_all(&path);
                SearchIndex::new(&path, &fields)
                    .expect("Failed to create search index after cleanup")
            }
        };

        for row in &self.data.rows {
            index
                .index_document(&row.id, &self.schema.columns, &row.values)
                .expect("Failed to index document");
        }

        self.indexes.search = Some(Arc::new(Mutex::new(index)));
    }

    pub fn index_row(&self, row: &Row) -> Result<(), String> {
        if let Some(ref index) = self.indexes.search {
            index
                .lock()
                .unwrap()
                .index_document(&row.id, &self.schema.columns, &row.values)
                .map_err(|e| e.to_string())
        } else {
            Ok(())
        }
    }
}
