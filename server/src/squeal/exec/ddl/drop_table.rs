use super::super::{Executor, QueryResult};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::DropTable;
use crate::storage::WalRecord;

impl Executor {
    pub async fn exec_drop_table(
        &self,
        stmt: DropTable,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        let table_exists = {
            let db = self.db.read().await;
            db.state().tables.contains_key(&stmt.name)
        };

        if stmt.if_exists && !table_exists {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                transaction_id: tx_id.map(|s| s.to_string()),
                session: None,
            });
        }

        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::DropTable {
                tx_id: tx_id.map(|s| s.to_string()),
                name: stmt.name.clone(),
            })?;
        }

        self.mutate_state(tx_id, |state| {
            state
                .tables
                .remove(&stmt.name)
                .ok_or_else(|| ExecError::TableNotFound(stmt.name.clone()))?;
            state.materialized_views.remove(&stmt.name);
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
