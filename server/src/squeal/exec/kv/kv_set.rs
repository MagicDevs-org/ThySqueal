use crate::squeal::exec::Executor;
use crate::squeal::exec::{ExecError, ExecResult};
use std::collections::HashSet;

impl Executor {
    pub async fn kv_set_add(
        &self,
        key: String,
        members: Vec<String>,
        tx_id: Option<&str>,
    ) -> ExecResult<usize> {
        let count = members.len();
        self.mutate_state(tx_id, |state| {
            let set = state.kv_set.entry(key).or_insert_with(HashSet::new);
            set.extend(members);
            self.refresh_materialized_views(state)?;
            Ok(count)
        })
        .await
    }

    pub async fn kv_set_members(&self, key: &str, tx_id: Option<&str>) -> ExecResult<Vec<String>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?;
            Ok(state
                .kv_set
                .get(key)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default())
        } else {
            let db = self.db.read().await;
            Ok(db
                .state()
                .kv_set
                .get(key)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default())
        }
    }

    pub async fn kv_set_is_member(
        &self,
        key: &str,
        member: &str,
        tx_id: Option<&str>,
    ) -> ExecResult<bool> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?;
            Ok(state
                .kv_set
                .get(key)
                .map(|s| s.contains(member))
                .unwrap_or(false))
        } else {
            let db = self.db.read().await;
            Ok(db
                .state()
                .kv_set
                .get(key)
                .map(|s| s.contains(member))
                .unwrap_or(false))
        }
    }

    pub async fn kv_set_remove(
        &self,
        key: String,
        members: Vec<String>,
        tx_id: Option<&str>,
    ) -> ExecResult<usize> {
        let count = members.len();
        self.mutate_state(tx_id, |state| {
            if let Some(set) = state.kv_set.get_mut(&key) {
                for member in members {
                    set.remove(&member);
                }
                if set.is_empty() {
                    state.kv_set.remove(&key);
                }
            }
            self.refresh_materialized_views(state)?;
            Ok(count)
        })
        .await
    }
}
