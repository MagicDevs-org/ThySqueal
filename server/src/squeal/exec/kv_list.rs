use super::{Executor, helpers};
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::storage::Value;

impl Executor {
    pub async fn kv_list_push(
        &self,
        key: String,
        values: Vec<Value>,
        left: bool,
        tx_id: Option<&str>,
    ) -> SqlResult<usize> {
        let count = values.len();
        self.mutate_state(tx_id, |state| {
            let list = state.kv_list.entry(key).or_insert_with(Vec::new);
            if left {
                for v in values.iter().rev() {
                    list.insert(0, v.clone());
                }
            } else {
                list.extend(values);
            }
            self.refresh_materialized_views(state)?;
            Ok(count)
        })
        .await
    }

    pub async fn kv_list_range(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        tx_id: Option<&str>,
    ) -> SqlResult<Vec<Value>> {
        let list = if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            state.kv_list.get(key).cloned().unwrap_or_default()
        } else {
            let db = self.db.read().await;
            db.state().kv_list.get(key).cloned().unwrap_or_default()
        };
        Ok(helpers::range_slice(&list, start, stop))
    }

    pub async fn kv_list_pop(
        &self,
        key: String,
        count: usize,
        left: bool,
        tx_id: Option<&str>,
    ) -> SqlResult<Vec<Value>> {
        self.mutate_state(tx_id, |state| {
            let mut vals = vec![];
            if let Some(list) = state.kv_list.get_mut(&key) {
                for _ in 0..count {
                    let val = if left {
                        if !list.is_empty() {
                            Some(list.remove(0))
                        } else {
                            None
                        }
                    } else {
                        list.pop()
                    };
                    if let Some(v) = val {
                        vals.push(v);
                    } else {
                        break;
                    }
                }
            }
            Ok(vals)
        })
        .await
    }

    pub async fn kv_list_len(&self, key: &str, tx_id: Option<&str>) -> SqlResult<usize> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            Ok(state.kv_list.get(key).map(|l| l.len()).unwrap_or(0))
        } else {
            let db = self.db.read().await;
            Ok(db.state().kv_list.get(key).map(|l| l.len()).unwrap_or(0))
        }
    }
}
