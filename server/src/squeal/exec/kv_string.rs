use super::Executor;
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::storage::{Value, WalRecord};

impl Executor {
    pub async fn kv_set(&self, key: String, value: Value, tx_id: Option<&str>) -> SqlResult<()> {
        self.mutate_state(tx_id, |state| {
            state.kv.insert(key.clone(), value.clone());
            self.refresh_materialized_views(state)?;
            Ok(())
        })
        .await?;

        let db = self.db.read().await;
        db.log_operation(&WalRecord::KvSet {
            tx_id: tx_id.map(|s| s.to_string()),
            key,
            value,
        })
        .map_err(SqlError::Storage)?;
        Ok(())
    }

    pub async fn kv_get(&self, key: &str, tx_id: Option<&str>) -> SqlResult<Option<Value>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            Ok(state.kv.get(key).cloned())
        } else {
            let db = self.db.read().await;
            Ok(db.state().kv.get(key).cloned())
        }
    }

    pub async fn kv_del(&self, key: String, tx_id: Option<&str>) -> SqlResult<()> {
        self.mutate_state(tx_id, |state| {
            state.kv.remove(&key);
            state.kv_expiry.remove(&key);
            self.refresh_materialized_views(state)?;
            Ok(())
        })
        .await?;

        let db = self.db.read().await;
        db.log_operation(&WalRecord::KvDelete {
            tx_id: tx_id.map(|s| s.to_string()),
            key,
        })
        .map_err(SqlError::Storage)?;
        Ok(())
    }

    pub async fn kv_exists(&self, key: &str, tx_id: Option<&str>) -> SqlResult<bool> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            if let Some(expiry) = state.kv_expiry.get(key)
                && *expiry < now
            {
                return Ok(false);
            }
            Ok(state.kv.contains_key(key))
        } else {
            let db = self.db.read().await;
            let state = db.state();
            if let Some(expiry) = state.kv_expiry.get(key)
                && *expiry < now
            {
                return Ok(false);
            }
            Ok(state.kv.contains_key(key))
        }
    }

    pub async fn kv_expire(
        &self,
        key: String,
        seconds: u64,
        tx_id: Option<&str>,
    ) -> SqlResult<bool> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let expiry = now + (seconds * 1000);

        let exists = self.kv_exists(&key, tx_id).await?;
        if !exists {
            return Ok(false);
        }

        self.mutate_state(tx_id, |state| {
            state.kv_expiry.insert(key.clone(), expiry);
            Ok(())
        })
        .await?;

        let db = self.db.read().await;
        db.log_operation(&WalRecord::KvExpire {
            tx_id: tx_id.map(|s| s.to_string()),
            key,
            expiry,
        })
        .map_err(SqlError::Storage)?;
        Ok(true)
    }

    pub async fn kv_ttl(&self, key: &str, tx_id: Option<&str>) -> SqlResult<i64> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let ttl = if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            state
                .kv_expiry
                .get(key)
                .map(|&expiry| expiry.saturating_sub(now) as i64 / 1000)
                .unwrap_or(-1)
        } else {
            let db = self.db.read().await;
            let state = db.state();
            state
                .kv_expiry
                .get(key)
                .map(|&expiry| expiry.saturating_sub(now) as i64 / 1000)
                .unwrap_or(-1)
        };
        Ok(ttl)
    }

    pub async fn kv_keys(&self, pattern: &str, tx_id: Option<&str>) -> SqlResult<Vec<String>> {
        let keys: Vec<String> = if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            state.kv.keys().cloned().collect()
        } else {
            let db = self.db.read().await;
            db.state().kv.keys().cloned().collect()
        };

        if pattern.is_empty() || pattern == "*" {
            return Ok(keys);
        }

        let regex_pattern = pattern.replace("?", ".").replace("*", ".*");
        let re = regex::Regex::new(&format!("^{}$", regex_pattern))
            .map_err(|e| SqlError::Runtime(format!("Invalid pattern: {}", e)))?;

        Ok(keys.into_iter().filter(|k| re.is_match(k)).collect())
    }
}
