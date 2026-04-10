use super::Executor;
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::storage::Value;
use std::collections::HashMap;

impl Executor {
    pub async fn kv_hash_set(
        &self,
        key: String,
        field: String,
        value: Value,
        tx_id: Option<&str>,
    ) -> SqlResult<()> {
        self.mutate_state(tx_id, |state| {
            state
                .kv_hash
                .entry(key.clone())
                .or_insert_with(HashMap::new)
                .insert(field, value);
            self.refresh_materialized_views(state)?;
            Ok(())
        })
        .await
    }

    pub async fn kv_hash_get(
        &self,
        key: &str,
        field: &str,
        tx_id: Option<&str>,
    ) -> SqlResult<Option<Value>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            Ok(state.kv_hash.get(key).and_then(|h| h.get(field)).cloned())
        } else {
            let db = self.db.read().await;
            Ok(db
                .state()
                .kv_hash
                .get(key)
                .and_then(|h| h.get(field))
                .cloned())
        }
    }

    pub async fn kv_hash_get_all(
        &self,
        key: &str,
        tx_id: Option<&str>,
    ) -> SqlResult<HashMap<String, Value>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            Ok(state.kv_hash.get(key).cloned().unwrap_or_default())
        } else {
            let db = self.db.read().await;
            Ok(db.state().kv_hash.get(key).cloned().unwrap_or_default())
        }
    }

    pub async fn kv_hash_del(
        &self,
        key: String,
        fields: Vec<String>,
        tx_id: Option<&str>,
    ) -> SqlResult<usize> {
        let count = fields.len();
        self.mutate_state(tx_id, |state| {
            if let Some(hash) = state.kv_hash.get_mut(&key) {
                for field in fields {
                    hash.remove(&field);
                }
                if hash.is_empty() {
                    state.kv_hash.remove(&key);
                }
            }
            self.refresh_materialized_views(state)?;
            Ok(count)
        })
        .await
    }
}
