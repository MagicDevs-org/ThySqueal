use super::super::{Executor, QueryResult};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::{CreateView, DropView};
use crate::storage::View;

impl Executor {
    pub async fn exec_create_view(
        &self,
        stmt: CreateView,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            if state.views.contains_key(&stmt.name) {
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "View {} already exists",
                        stmt.name
                    )),
                ));
            }

            state.views.insert(
                stmt.name.clone(),
                View {
                    name: stmt.name.clone(),
                    query: stmt.query.clone(),
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

    pub async fn exec_drop_view(&self, stmt: DropView) -> ExecResult<QueryResult> {
        let name = stmt.name.clone();
        self.mutate_state(None, |state| {
            if state.views.remove(&name).is_none() {
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "View {} does not exist",
                        stmt.name
                    )),
                ));
            }
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }
}
