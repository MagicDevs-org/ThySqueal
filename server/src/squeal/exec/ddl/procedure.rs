use super::super::{Executor, QueryResult, Session};
use crate::squeal::eval::{EvalContext, evaluate_expression_joined};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::{Call, CreateFunction, CreateProcedure, DropFunction, DropProcedure};

impl Executor {
    pub async fn exec_call(&self, stmt: Call) -> ExecResult<QueryResult> {
        let db = self.db.read().await;
        let (params, body) = db
            .state()
            .procedures
            .get(&stmt.name)
            .map(|(p, b)| (p.clone(), b.clone()))
            .or_else(|| {
                db.state()
                    .functions
                    .get(&stmt.name)
                    .map(|(p, b)| (p.clone(), b.clone()))
            })
            .ok_or_else(|| {
                ExecError::Runtime(format!("Function/Procedure {} not found", stmt.name))
            })?;

        let db_state = db.state();
        let mut session = Session::root();
        for (i, arg) in stmt.args.iter().enumerate() {
            if i < params.len() {
                let param_name = format!("@{}", params[i].name);
                let eval_ctx = EvalContext::new(&[], &[], &[], db_state).with_session(&session);
                if let Ok(value) = evaluate_expression_joined(self, arg, &eval_ctx) {
                    session.variables.insert(param_name, value);
                }
            }
        }
        drop(db);

        self.exec_squeal(body, vec![], session).await
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
            state
                .procedures
                .insert(stmt.name.clone(), (stmt.params, *stmt.body));
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

    pub async fn exec_create_function(
        &self,
        stmt: CreateFunction,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            if state.functions.contains_key(&stmt.name) {
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Function {} already exists",
                        stmt.name
                    )),
                ));
            }
            state
                .functions
                .insert(stmt.name.clone(), (stmt.params, *stmt.body));
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

    pub async fn exec_drop_function(&self, stmt: DropFunction) -> ExecResult<QueryResult> {
        let name = stmt.name.clone();
        self.mutate_state(None, |state| {
            if state.functions.remove(&name).is_none() {
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Function {} does not exist",
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
