use crate::squeal::exec::Executor;
use crate::squeal::exec::helpers;
use crate::squeal::exec::{ExecError, ExecResult};
use crate::storage::Value;

impl Executor {
    pub async fn kv_zset_add(
        &self,
        key: String,
        members: Vec<(f64, String)>,
        tx_id: Option<&str>,
    ) -> ExecResult<usize> {
        let count = members.len();
        self.mutate_state(tx_id, |state| {
            let zset = state.kv_zset.entry(key).or_insert_with(Vec::new);
            for (score, member) in members {
                zset.push((score, member));
            }
            self.refresh_materialized_views(state)?;
            Ok(count)
        })
        .await
    }

    pub async fn kv_zset_range(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
        tx_id: Option<&str>,
    ) -> ExecResult<Vec<Value>> {
        let zset = if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?;
            state.kv_zset.get(key).cloned().unwrap_or_default()
        } else {
            let db = self.db.read().await;
            db.state().kv_zset.get(key).cloned().unwrap_or_default()
        };
        Ok(helpers::zset_range(zset, start, stop, with_scores))
    }

    pub async fn kv_zsetrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
        with_scores: bool,
        tx_id: Option<&str>,
    ) -> ExecResult<Vec<Value>> {
        let zset = if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?;
            state.kv_zset.get(key).cloned().unwrap_or_default()
        } else {
            let db = self.db.read().await;
            db.state().kv_zset.get(key).cloned().unwrap_or_default()
        };
        Ok(helpers::zset_filter_by_score(zset, min, max, with_scores))
    }

    pub async fn kv_zset_remove(
        &self,
        key: String,
        members: Vec<String>,
        tx_id: Option<&str>,
    ) -> ExecResult<usize> {
        let count = members.len();
        self.mutate_state(tx_id, |state| {
            if let Some(zset) = state.kv_zset.get_mut(&key) {
                let members_set: std::collections::HashSet<_> = members.iter().cloned().collect();
                zset.retain(|(_, m)| !members_set.contains(m));
                if zset.is_empty() {
                    state.kv_zset.remove(&key);
                }
            }
            self.refresh_materialized_views(state)?;
            Ok(count)
        })
        .await
    }
}
