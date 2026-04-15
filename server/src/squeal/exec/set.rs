use super::{ExecutionContext, Executor, QueryResult};
use crate::squeal::eval::{EvalContext, evaluate_expression_joined};
use crate::squeal::exec::ExecResult;
use crate::squeal::ir::Set;

impl Executor {
    pub async fn exec_set(&self, stmt: Set, ctx: &ExecutionContext) -> ExecResult<QueryResult> {
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
                crate::squeal::ir::Expression::Variable(v) => {
                    if v.is_system {
                        // TODO: Handle system variables (most are read-only for now)
                    } else {
                        session.variables.insert(v.name.clone(), value);
                    }
                }
                crate::squeal::ir::Expression::Column(c) => {
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
