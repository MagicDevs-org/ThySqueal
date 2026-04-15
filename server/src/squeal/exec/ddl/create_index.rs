use super::super::{Executor, QueryResult};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::{CreateIndex, IndexType};
use crate::storage::WalRecord;

impl Executor {
    pub async fn exec_create_index(
        &self,
        stmt: CreateIndex,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        let table_name = stmt.table.clone();
        let index_name = stmt.name.clone();

        // 1. Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateIndex {
                tx_id: tx_id.map(|s| s.to_string()),
                table: table_name.clone(),
                name: index_name.clone(),
                expressions: stmt.expressions.clone(),
                unique: stmt.unique,
                use_hash: matches!(stmt.index_type, IndexType::Hash),
                where_clause: stmt.where_clause.clone(),
            })?;
        }

        // 2. Mutate state
        self.mutate_state(tx_id, |state| {
            let db_state_copy = state.clone();
            let table = state
                .get_table_mut(&table_name)
                .ok_or_else(|| ExecError::TableNotFound(table_name.clone()))?;

            table.create_index(
                self,
                index_name,
                stmt.expressions,
                stmt.unique,
                matches!(stmt.index_type, IndexType::Hash),
                stmt.where_clause,
                &db_state_copy,
            )?;
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
