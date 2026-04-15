use super::super::{Executor, QueryResult};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::CreateDatabase;
use crate::storage::WalRecord;

impl Executor {
    pub async fn exec_create_database(
        &self,
        stmt: CreateDatabase,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        let name = stmt.name.clone();

        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateDatabase {
                tx_id: tx_id.map(|s| s.to_string()),
                name: name.clone(),
            })?;
        }

        self.mutate_state(tx_id, |state| {
            if state.databases.contains_key(&name) {
                if stmt.if_not_exists {
                    return Ok(());
                }
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Database {} already exists",
                        name
                    )),
                ));
            }

            state
                .databases
                .insert(name.clone(), crate::storage::DatabaseData::new());
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
