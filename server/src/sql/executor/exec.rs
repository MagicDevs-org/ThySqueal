use super::{Executor, QueryResult};
use crate::sql::error::SqlResult;
use crate::squeal;
use crate::storage::Value;

impl Executor {
    pub async fn exec_kv_set(
        &self,
        kv: squeal::KvSet,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let key = kv.key.clone();
        self.kv_set(kv.key, kv.value, tx_id).await?;
        if let Some(exp) = kv.expiry {
            self.kv_expire(key, exp, tx_id).await?;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_get(
        &self,
        kv: squeal::KvGet,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let value = self.kv_get(&kv.key, tx_id).await?;
        let row = match &value {
            Some(v) => vec![v.clone()],
            None => vec![],
        };
        Ok(QueryResult {
            columns: vec!["value".to_string()],
            rows: if value.is_some() { vec![row] } else { vec![] },
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_del(
        &self,
        kv: squeal::KvDel,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let mut count = 0;
        for key in kv.keys {
            if self.kv_get(&key, tx_id).await?.is_some() {
                self.kv_del(key, tx_id).await?;
                count += 1;
            }
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: count,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_hash_set(
        &self,
        kv: squeal::KvHashSet,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        self.kv_hash_set(kv.key, kv.field, kv.value, tx_id).await?;
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_hash_get(
        &self,
        kv: squeal::KvHashGet,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let value = self.kv_hash_get(&kv.key, &kv.field, tx_id).await?;
        let row = match &value {
            Some(v) => vec![v.clone()],
            None => vec![],
        };
        Ok(QueryResult {
            columns: vec!["value".to_string()],
            rows: if value.is_some() { vec![row] } else { vec![] },
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_list_push(
        &self,
        kv: squeal::KvListPush,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let count = self.kv_list_push(kv.key, kv.values, kv.left, tx_id).await?;
        Ok(QueryResult {
            columns: vec!["count".to_string()],
            rows: vec![vec![Value::Int(count as i64)]],
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_list_range(
        &self,
        kv: squeal::KvListRange,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let values = self
            .kv_list_range(&kv.key, kv.start, kv.stop, tx_id)
            .await?;
        let rows: Vec<Vec<Value>> = values.into_iter().map(|v| vec![v]).collect();
        Ok(QueryResult {
            columns: vec!["value".to_string()],
            rows,
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_set_add(
        &self,
        kv: squeal::KvSetAdd,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let count = self.kv_set_add(kv.key, kv.members, tx_id).await?;
        Ok(QueryResult {
            columns: vec!["count".to_string()],
            rows: vec![vec![Value::Int(count as i64)]],
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_set_members(
        &self,
        kv: squeal::KvSetMembers,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let members = self.kv_set_members(&kv.key, tx_id).await?;
        let rows: Vec<Vec<Value>> = members.into_iter().map(|m| vec![Value::Text(m)]).collect();
        Ok(QueryResult {
            columns: vec!["member".to_string()],
            rows,
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_zset_add(
        &self,
        kv: squeal::KvZSetAdd,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let count = self.kv_zset_add(kv.key, kv.members, tx_id).await?;
        Ok(QueryResult {
            columns: vec!["count".to_string()],
            rows: vec![vec![Value::Int(count as i64)]],
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_zset_range(
        &self,
        kv: squeal::KvZSetRange,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let values = self
            .kv_zset_range(&kv.key, kv.start, kv.stop, kv.with_scores, tx_id)
            .await?;
        let rows: Vec<Vec<Value>> = values
            .chunks(if kv.with_scores { 2 } else { 1 })
            .map(|chunk| chunk.to_vec())
            .collect();
        Ok(QueryResult {
            columns: if kv.with_scores {
                vec!["member".to_string(), "score".to_string()]
            } else {
                vec!["member".to_string()]
            },
            rows,
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_stream_add(
        &self,
        kv: squeal::KvStreamAdd,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let id = self.kv_stream_add(kv.key, kv.id, kv.fields, tx_id).await?;
        Ok(QueryResult {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::Text(id)]],
            rows_affected: 1,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_stream_range(
        &self,
        kv: squeal::KvStreamRange,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let results = self
            .kv_stream_range(&kv.key, &kv.start, &kv.stop, kv.count, tx_id)
            .await?;
        let mut rows = vec![];
        for (id, fields) in results {
            let mut row = vec![Value::Text(id)];
            for (_, v) in fields {
                row.push(v);
            }
            rows.push(row);
        }
        let mut columns = vec!["id".to_string()];
        if !rows.is_empty() && rows[0].len() > 1 {
            for i in 1..rows[0].len() {
                columns.push(format!("field{}", i - 1));
            }
        }
        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_kv_stream_len(
        &self,
        kv: squeal::KvStreamLen,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let len = self.kv_stream_len(&kv.key, tx_id).await?;
        Ok(QueryResult {
            columns: vec!["length".to_string()],
            rows: vec![vec![Value::Int(len as i64)]],
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_pubsub_publish(&self, kv: squeal::PubSubPublish) -> SqlResult<QueryResult> {
        let count = self.pubsub_publish(kv.channel, kv.message).await?;
        Ok(QueryResult {
            columns: vec!["subscribers".to_string()],
            rows: vec![vec![Value::Int(count as i64)]],
            rows_affected: 1,
            transaction_id: None,
            session: None,
        })
    }
}
