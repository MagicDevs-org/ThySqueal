pub mod binary;
pub mod function;
pub mod subquery;

use super::column::resolve_column;
use super::{EvalContext, Evaluator};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::Expression;
use crate::storage::Value;

pub fn evaluate_expression_joined(
    executor: &dyn Evaluator,
    expr: &Expression,
    ctx: &EvalContext<'_>,
) -> ExecResult<Value> {
    match expr {
        Expression::Literal(v) => Ok(v.clone()),
        Expression::Placeholder(i) => {
            if *i == 0 {
                return Err(ExecError::Runtime(
                    "Positional placeholder '?' was not correctly numbered".to_string(),
                ));
            }
            ctx.params.get(*i - 1).cloned().ok_or_else(|| {
                ExecError::Runtime(format!("Missing parameter for placeholder ${}", i))
            })
        }
        Expression::Column(name) => {
            if let Ok(val) = resolve_column(name, ctx.contexts) {
                return Ok(val);
            }
            if let Ok(val) = resolve_column(name, ctx.outer_contexts) {
                return Ok(val);
            }

            Err(ExecError::ColumnNotFound(name.clone()))
        }
        Expression::Subquery(subquery) => subquery::evaluate_subquery(
            executor,
            subquery,
            ctx.contexts,
            ctx.params,
            ctx.outer_contexts,
            ctx.db_state,
        ),
        Expression::BinaryOp(left, op, right) => {
            let l = evaluate_expression_joined(executor, left, ctx)?;
            let r = evaluate_expression_joined(executor, right, ctx)?;
            binary::evaluate_binary_op(l, op, r)
        }
        Expression::ScalarFunc(sf) => {
            let mut eval_args = Vec::new();
            for arg in &sf.args {
                eval_args.push(evaluate_expression_joined(executor, arg, ctx)?);
            }
            function::evaluate_scalar_func(&sf.name, &eval_args)
        }
        Expression::FunctionCall(_) => Err(ExecError::Runtime(
            "Aggregate functions must be evaluated at the top level".to_string(),
        )),
        Expression::Star => Err(ExecError::Runtime(
            "Star expression must be evaluated at the top level".to_string(),
        )),
        Expression::Variable(v) => {
            if v.is_system {
                Ok(get_system_variable(&v.name))
            } else if let Some(session) = ctx.session {
                Ok(session
                    .variables
                    .get(&v.name)
                    .cloned()
                    .unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        Expression::UnaryNot(e) => {
            let val = evaluate_expression_joined(executor, e, ctx)?;
            match val {
                Value::Bool(b) => Ok(Value::Bool(!b)),
                Value::Null => Ok(Value::Null),
                _ => Err(ExecError::TypeMismatch(
                    "NOT requires boolean value".to_string(),
                )),
            }
        }
        Expression::WindowFunc(_) => Err(ExecError::Runtime(
            "Window functions must be evaluated at the top level".to_string(),
        )),
        Expression::CaseWhen(cw) => {
            for (cond, then_val) in &cw.conditions {
                let cond_result = evaluate_expression_joined(executor, cond, ctx)?;
                if is_truthy(&cond_result) {
                    return evaluate_expression_joined(executor, then_val, ctx);
                }
            }
            if let Some(else_expr) = &cw.else_expr {
                evaluate_expression_joined(executor, else_expr, ctx)
            } else {
                Ok(Value::Null)
            }
        }
    }
}

pub fn get_system_variable(name: &str) -> Value {
    match name.to_lowercase().as_str() {
        "version" => Value::Text("0.8.0-ThySqueal".to_string()),
        "version_comment" => Value::Text("ThySqueal".to_string()),
        "max_allowed_packet" => Value::Int(67108864),
        "auto_increment_increment" => Value::Int(1),
        "character_set_client" => Value::Text("utf8mb4".to_string()),
        "character_set_connection" => Value::Text("utf8mb4".to_string()),
        "character_set_results" => Value::Text("utf8mb4".to_string()),
        "character_set_server" => Value::Text("utf8mb4".to_string()),
        "collation_connection" => Value::Text("utf8mb4_general_ci".to_string()),
        "collation_server" => Value::Text("utf8mb4_general_ci".to_string()),
        "interactive_timeout" => Value::Int(28800),
        "wait_timeout" => Value::Int(28800),
        "net_write_timeout" => Value::Int(60),
        "net_read_timeout" => Value::Int(30),
        "time_zone" => Value::Text("SYSTEM".to_string()),
        "system_time_zone" => Value::Text("UTC".to_string()),
        _ => Value::Null,
    }
}

fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Bool(b) => *b,
        Value::Int(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        Value::Text(s) => !s.is_empty(),
        Value::Null => false,
        Value::Json(j) => !j.is_null(),
        Value::DateTime(_) => true,
    }
}
