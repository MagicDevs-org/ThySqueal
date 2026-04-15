use super::super::{Executor, QueryResult};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::CreateTrigger;
use crate::storage::{Trigger, WalRecord};

impl Executor {
    pub async fn exec_create_trigger(
        &self,
        stmt: CreateTrigger,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        let name = stmt.name.clone();

        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateTrigger {
                tx_id: tx_id.map(|s| s.to_string()),
                name: name.clone(),
                timing: stmt.timing.clone(),
                event: stmt.event.clone(),
                table: stmt.table.clone(),
                body: stmt.body.clone(),
            })?;
        }

        self.mutate_state(tx_id, |state| {
            if state.triggers.contains_key(&name) {
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Trigger {} already exists",
                        name
                    )),
                ));
            }

            let trigger_name = name.clone();
            state.triggers.insert(
                trigger_name.clone(),
                Trigger {
                    name: trigger_name,
                    timing: stmt.timing.clone(),
                    event: stmt.event.clone(),
                    table: stmt.table.clone(),
                    body: stmt.body.clone(),
                },
            );
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
