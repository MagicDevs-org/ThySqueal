use super::super::{Executor, QueryResult};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::DropTrigger;
use crate::storage::WalRecord;

impl Executor {
    pub async fn exec_drop_trigger(
        &self,
        stmt: DropTrigger,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        let name = stmt.name.clone();

        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::DropTrigger {
                tx_id: tx_id.map(|s| s.to_string()),
                name: name.clone(),
            })?;
        }

        self.mutate_state(tx_id, |state| {
            state.triggers.remove(&name).ok_or_else(|| {
                ExecError::Storage(crate::storage::error::StorageError::PersistenceError(
                    format!("Trigger {} does not exist", name),
                ))
            })?;
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
            session: None,
        })
    }
}
