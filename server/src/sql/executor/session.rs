use super::super::error::SqlResult;
use super::{ExecutionContext, Executor, QueryResult};
use crate::sql::eval::{EvalContext, evaluate_expression_joined};
use crate::squeal::{Expression, Set};

impl Executor {
    pub(crate) async fn exec_set(
        &self,
        stmt: Set,
        ctx: &ExecutionContext,
    ) -> SqlResult<QueryResult> {
        let mut session = ctx.session.clone();

        for (var_expr, val_expr) in &stmt.assignments {
            let db = self.db.read().await;
            let state = if let Some(id) = &session.transaction_id {
                self.transactions
                    .get(id)
                    .map(|s| s.clone())
                    .unwrap_or_else(|| db.state().clone())
            } else {
                db.state().clone()
            };

            let eval_ctx =
                EvalContext::new(&[], &ctx.params, &[], &state).with_session(&ctx.session);
            let value = evaluate_expression_joined(self, val_expr, &eval_ctx)?;

            match var_expr {
                Expression::Variable(v) => {
                    if v.is_system {
                        // TODO: Handle system variables (most are read-only for now)
                        // For now just ignore or return error if not found
                    } else {
                        session.variables.insert(v.name.clone(), value);
                    }
                }
                Expression::Column(c) => {
                    // MySQL also allows SET var = val where var is session var
                    session.variables.insert(c.clone(), value);
                }
                _ => {}
            }
        }

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: session.transaction_id.clone(),
            session: Some(session),
        })
    }
}
