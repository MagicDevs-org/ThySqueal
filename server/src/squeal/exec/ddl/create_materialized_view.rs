use super::super::{Executor, QueryResult, SelectQueryPlan, Session};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::CreateMaterializedView;
use crate::storage::{Table, WalRecord};

impl Executor {
    pub async fn exec_create_materialized_view(
        &self,
        stmt: CreateMaterializedView,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        // 1. Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateMaterializedView {
                tx_id: tx_id.map(|s| s.to_string()),
                name: stmt.name.clone(),
                query: Box::new(stmt.query.clone()),
            })?;
        }

        self.mutate_state(tx_id, |state| {
            if state.tables.contains_key(&stmt.name) {
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Table {} already exists",
                        stmt.name
                    )),
                ));
            }

            let plan = SelectQueryPlan::new(stmt.query.clone(), state, Session::root());
            let res = futures::executor::block_on(self.exec_select_recursive(plan))?;

            let mut cols = Vec::new();
            for col_name in &res.columns {
                cols.push(crate::storage::Column {
                    name: col_name.clone(),
                    data_type: crate::storage::DataType::Text,
                    is_auto_increment: false,
                    is_not_null: false,
                    default_value: None,
                });
            }

            let mut table = Table::new(stmt.name.clone(), cols, None, vec![]);
            table.data.rows = res
                .rows
                .into_iter()
                .enumerate()
                .map(|(i, values)| crate::storage::Row {
                    id: format!("mv_{}_{}", stmt.name, i),
                    values,
                })
                .collect();

            state.tables.insert(stmt.name.clone(), table);
            state
                .materialized_views
                .insert(stmt.name.clone(), stmt.query.clone());
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
