use super::super::{Executor, QueryResult, Session};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::{Call, CreateProcedure, DropProcedure};

impl Executor {
    pub async fn exec_call(&self, stmt: Call) -> ExecResult<QueryResult> {
        let db = self.db.read().await;
        let procedure = db
            .state()
            .procedures
            .get(&stmt.name)
            .ok_or_else(|| ExecError::Runtime(format!("Procedure {} not found", stmt.name)))?
            .clone();
        drop(db);
        self.exec_squeal(procedure, vec![], Session::root()).await
    }

    pub async fn exec_create_procedure(
        &self,
        stmt: CreateProcedure,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            if state.procedures.contains_key(&stmt.name) {
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Procedure {} already exists",
                        stmt.name
                    )),
                ));
            }
            state.procedures.insert(stmt.name.clone(), *stmt.body);
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

    pub async fn exec_drop_procedure(&self, stmt: DropProcedure) -> ExecResult<QueryResult> {
        let name = stmt.name.clone();
        self.mutate_state(None, |state| {
            if state.procedures.remove(&name).is_none() {
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Procedure {} does not exist",
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
