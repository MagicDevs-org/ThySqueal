use crate::storage::Value;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
pub enum TableIndex {
    BTree {
        unique: bool,
        expressions: Vec<serde_json::Value>,
        where_clause: Option<serde_json::Value>,
        data: BTreeMap<Vec<Value>, Vec<String>>,
    },
    Hash {
        unique: bool,
        expressions: Vec<serde_json::Value>,
        where_clause: Option<serde_json::Value>,
        data: HashMap<Vec<Value>, Vec<String>>,
    },
}

impl Serialize for TableIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TableIndex::BTree {
                unique,
                expressions,
                where_clause,
                data,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_key("BTree")?;
                map.serialize_value(&BTreeInner {
                    unique,
                    expressions,
                    where_clause,
                    data,
                })?;
                map.end()
            }
            TableIndex::Hash {
                unique,
                expressions,
                where_clause,
                data,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_key("Hash")?;
                map.serialize_value(&HashInner {
                    unique,
                    expressions,
                    where_clause,
                    data,
                })?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for TableIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let json_value = serde_json::Value::deserialize(deserializer)?;

        if let Some(btree_obj) = json_value.get("BTree").and_then(|v| v.as_object()) {
            let unique = btree_obj
                .get("unique")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let expressions = btree_obj
                .get("expressions")
                .map(|v| serde_json::from_value(v.clone()).unwrap_or_default())
                .unwrap_or_default();
            let where_clause = btree_obj
                .get("where_clause")
                .and_then(|v| if v.is_null() { None } else { Some(v.clone()) })
                .and_then(|v| serde_json::from_value(v).ok());
            let data = if let Some(data_obj) = btree_obj.get("data").and_then(|v| v.as_object()) {
                let mut map = BTreeMap::new();
                for (k, v) in data_obj {
                    #[allow(clippy::collapsible_if)]
                    if let Ok(key) = serde_json::from_str::<Vec<Value>>(k) {
                        if let Ok(val) = serde_json::from_value(v.clone()) {
                            map.insert(key, val);
                        }
                    }
                }
                map
            } else {
                BTreeMap::new()
            };
            Ok(TableIndex::BTree {
                unique,
                expressions,
                where_clause,
                data,
            })
        } else if let Some(hash_obj) = json_value.get("Hash").and_then(|v| v.as_object()) {
            let unique = hash_obj
                .get("unique")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let expressions = hash_obj
                .get("expressions")
                .map(|v| serde_json::from_value(v.clone()).unwrap_or_default())
                .unwrap_or_default();
            let where_clause = hash_obj
                .get("where_clause")
                .and_then(|v| if v.is_null() { None } else { Some(v.clone()) })
                .and_then(|v| serde_json::from_value(v).ok());
            let data = if let Some(data_obj) = hash_obj.get("data").and_then(|v| v.as_object()) {
                let mut map = HashMap::new();
                for (k, v) in data_obj {
                    #[allow(clippy::collapsible_if)]
                    if let Ok(key) = serde_json::from_str::<Vec<Value>>(k) {
                        if let Ok(val) = serde_json::from_value(v.clone()) {
                            map.insert(key, val);
                        }
                    }
                }
                map
            } else {
                HashMap::new()
            };
            Ok(TableIndex::Hash {
                unique,
                expressions,
                where_clause,
                data,
            })
        } else {
            Err(serde::de::Error::custom("Expected BTree or Hash variant"))
        }
    }
}

struct BTreeInner<'a> {
    unique: &'a bool,
    expressions: &'a Vec<serde_json::Value>,
    where_clause: &'a Option<serde_json::Value>,
    data: &'a BTreeMap<Vec<Value>, Vec<String>>,
}

impl Serialize for BTreeInner<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("unique", self.unique)?;
        map.serialize_entry("expressions", self.expressions)?;
        map.serialize_entry("where_clause", self.where_clause)?;
        let mut data_map = serde_json::Map::new();
        for (key, val) in self.data {
            let key_str = serde_json::to_string(key).map_err(serde::ser::Error::custom)?;
            data_map.insert(key_str, serde_json::json!(val));
        }
        map.serialize_entry("data", &serde_json::Value::Object(data_map))?;
        map.end()
    }
}

struct HashInner<'a> {
    unique: &'a bool,
    expressions: &'a Vec<serde_json::Value>,
    where_clause: &'a Option<serde_json::Value>,
    data: &'a HashMap<Vec<Value>, Vec<String>>,
}

impl Serialize for HashInner<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("unique", self.unique)?;
        map.serialize_entry("expressions", self.expressions)?;
        map.serialize_entry("where_clause", self.where_clause)?;
        let mut data_map = serde_json::Map::new();
        for (key, val) in self.data {
            let key_str = serde_json::to_string(key).map_err(serde::ser::Error::custom)?;
            data_map.insert(key_str, serde_json::json!(val));
        }
        map.serialize_entry("data", &serde_json::Value::Object(data_map))?;
        map.end()
    }
}

impl TableIndex {
    pub fn is_unique(&self) -> bool {
        match self {
            TableIndex::BTree { unique, .. } => *unique,
            TableIndex::Hash { unique, .. } => *unique,
        }
    }

    pub fn expressions(&self) -> Vec<crate::squeal::ir::Expression> {
        let exprs = match self {
            TableIndex::BTree { expressions, .. } => expressions,
            TableIndex::Hash { expressions, .. } => expressions,
        };
        exprs
            .iter()
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .collect()
    }

    pub fn where_clause(&self) -> Option<crate::squeal::ir::Condition> {
        let cond = match self {
            TableIndex::BTree { where_clause, .. } => where_clause,
            TableIndex::Hash { where_clause, .. } => where_clause,
        };
        cond.as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap())
    }

    pub fn key_count(&self) -> usize {
        match self {
            TableIndex::BTree { data, .. } => data.len(),
            TableIndex::Hash { data, .. } => data.len(),
        }
    }

    pub fn total_rows(&self) -> usize {
        match self {
            TableIndex::BTree { data, .. } => data.values().map(|v| v.len()).sum(),
            TableIndex::Hash { data, .. } => data.values().map(|v| v.len()).sum(),
        }
    }

    pub fn get(&self, key: &[Value]) -> Option<Vec<String>> {
        match self {
            TableIndex::BTree { data, .. } => data.get(key).cloned(),
            TableIndex::Hash { data, .. } => data.get(key).cloned(),
        }
    }

    pub fn insert(
        &mut self,
        key: Vec<Value>,
        row_id: String,
    ) -> Result<(), crate::storage::error::StorageError> {
        let unique = self.is_unique();
        match self {
            TableIndex::BTree { data, .. } => {
                let entry = data.entry(key.clone()).or_default();
                if unique && !entry.is_empty() && !entry.contains(&row_id) {
                    return Err(crate::storage::error::StorageError::DuplicateKey(format!(
                        "{:?}",
                        key
                    )));
                }
                if !entry.contains(&row_id) {
                    entry.push(row_id);
                }
            }
            TableIndex::Hash { data, .. } => {
                let entry = data.entry(key.clone()).or_default();
                if unique && !entry.is_empty() && !entry.contains(&row_id) {
                    return Err(crate::storage::error::StorageError::DuplicateKey(format!(
                        "{:?}",
                        key
                    )));
                }
                if !entry.contains(&row_id) {
                    entry.push(row_id);
                }
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, key: &[Value], row_id: &str) {
        match self {
            TableIndex::BTree { data, .. } => {
                if let Some(ids) = data.get_mut(key) {
                    ids.retain(|id| id != row_id);
                    if ids.is_empty() {
                        data.remove(key);
                    }
                }
            }
            TableIndex::Hash { data, .. } => {
                if let Some(ids) = data.get_mut(key) {
                    ids.retain(|id| id != row_id);
                    if ids.is_empty() {
                        data.remove(key);
                    }
                }
            }
        }
    }
}
