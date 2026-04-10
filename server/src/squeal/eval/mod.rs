pub mod column;
pub mod condition;
pub mod expression;

use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::squeal::ir::{Condition, Expression, Select};
use crate::storage::{DatabaseState, Row, Table, Value};
use futures::FutureExt;
use futures::future::BoxFuture;

pub use condition::evaluate_condition_joined;
pub use expression::evaluate_expression_joined;

/// Context for expression and condition evaluation
pub struct EvalContext<'a> {
    pub contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
    pub params: &'a [Value],
    pub outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
    pub db_state: &'a DatabaseState,
    pub session: Option<&'a crate::squeal::exec::Session>,
}

impl<'a> EvalContext<'a> {
    pub fn new(
        contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        params: &'a [Value],
        outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        db_state: &'a DatabaseState,
    ) -> Self {
        Self {
            contexts,
            params,
            outer_contexts,
            db_state,
            session: None,
        }
    }

    pub fn with_session(mut self, session: &'a crate::squeal::exec::Session) -> Self {
        self.session = Some(session);
        self
    }
}

/// Trait for evaluating expressions, implemented by Executor and RecoveryEvaluator
pub trait Evaluator: Send + Sync {
    fn exec_select_internal<'a>(
        &'a self,
        stmt: Select,
        outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        params: &'a [Value],
        db_state: &'a DatabaseState,
    ) -> BoxFuture<'a, SqlResult<crate::squeal::exec::QueryResult>>;
}

/// A simple evaluator used during WAL recovery when a full Executor is not yet available.
/// It does not support subqueries.
pub struct RecoveryEvaluator;

impl Evaluator for RecoveryEvaluator {
    fn exec_select_internal<'a>(
        &'a self,
        _stmt: Select,
        _outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        _params: &'a [Value],
        _db_state: &'a DatabaseState,
    ) -> BoxFuture<'a, SqlResult<crate::squeal::exec::QueryResult>> {
        async {
            Err(SqlError::Runtime(
                "Subqueries are not supported during WAL recovery".to_string(),
            ))
        }
        .boxed()
    }
}

#[allow(dead_code, clippy::too_many_arguments)]
pub fn evaluate_condition(
    executor: &dyn Evaluator,
    cond: &Condition,
    table: &Table,
    table_alias: Option<&str>,
    params: &[Value],
    row: &Row,
    db_state: &DatabaseState,
    session: &crate::squeal::exec::Session,
) -> SqlResult<bool> {
    let contexts = [(table, table_alias, row)];
    let ctx = EvalContext::new(&contexts, params, &[], db_state).with_session(session);
    evaluate_condition_joined(executor, cond, &ctx)
}

#[allow(dead_code, clippy::too_many_arguments)]
pub fn evaluate_expression(
    executor: &dyn Evaluator,
    expr: &Expression,
    table: &Table,
    table_alias: Option<&str>,
    params: &[Value],
    row: &Row,
    db_state: &DatabaseState,
    session: &crate::squeal::exec::Session,
) -> SqlResult<Value> {
    let contexts = [(table, table_alias, row)];
    let ctx = EvalContext::new(&contexts, params, &[], db_state).with_session(session);
    evaluate_expression_joined(executor, expr, &ctx)
}
