use crate::squeal::exec::Executor;
use crate::squeal::exec::{ExecError, ExecResult};
use crate::storage::Value;
use std::collections::HashMap;

impl Executor {
    pub async fn kv_stream_add(
        &self,
        key: String,
        id: Option<u64>,
        fields: HashMap<String, Value>,
        tx_id: Option<&str>,
    ) -> ExecResult<String> {
        let new_id = self
            .mutate_state(tx_id, |state| {
                let stream = state.kv_stream.entry(key.clone()).or_insert_with(Vec::new);
                let last_id = state.kv_stream_last_id.entry(key.clone()).or_insert(0);
                let new_id = id.unwrap_or(*last_id + 1);
                *last_id = new_id;
                stream.push((new_id, fields));
                Ok(new_id)
            })
            .await?;
        Ok(new_id.to_string())
    }

    pub async fn kv_stream_range(
        &self,
        key: &str,
        start: &str,
        stop: &str,
        count: Option<usize>,
        tx_id: Option<&str>,
    ) -> ExecResult<Vec<(String, HashMap<String, Value>)>> {
        let parse_id = |s: &str| -> ExecResult<u64> {
            if s == "-" {
                return Ok(0);
            }
            if s == "+" {
                return Ok(u64::MAX);
            }
            s.parse()
                .map_err(|_| ExecError::Runtime("Invalid stream ID".to_string()))
        };
        let start_id = parse_id(start)?;
        let stop_id = parse_id(stop)?;
        let stream = if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?;
            state.kv_stream.get(key).cloned().unwrap_or_default()
        } else {
            let db = self.db.read().await;
            db.state().kv_stream.get(key).cloned().unwrap_or_default()
        };
        let results: Vec<_> = stream
            .into_iter()
            .filter(|(id, _)| *id >= start_id && *id <= stop_id)
            .collect();
        let mut results = results;
        if let Some(c) = count {
            results.truncate(c);
        }
        Ok(results
            .into_iter()
            .map(|(id, fields)| (id.to_string(), fields))
            .collect())
    }

    pub async fn kv_stream_len(&self, key: &str, tx_id: Option<&str>) -> ExecResult<usize> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?;
            Ok(state.kv_stream.get(key).map(|s| s.len()).unwrap_or(0))
        } else {
            let db = self.db.read().await;
            Ok(db.state().kv_stream.get(key).map(|s| s.len()).unwrap_or(0))
        }
    }
}
