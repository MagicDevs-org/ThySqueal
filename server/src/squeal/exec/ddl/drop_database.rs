use super::super::{Executor, QueryResult};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::DropDatabase;
use crate::storage::WalRecord;

impl Executor {
    pub async fn exec_drop_database(
        &self,
        stmt: DropDatabase,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        let name = stmt.name.clone();

        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::DropDatabase {
                tx_id: tx_id.map(|s| s.to_string()),
                name: name.clone(),
            })?;
        }

        self.mutate_state(tx_id, |state| {
            if !state.databases.contains_key(&name) {
                if stmt.if_exists {
                    return Ok(());
                }
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Database {} does not exist",
                        name
                    )),
                ));
            }

            state.databases.remove(&name);
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
