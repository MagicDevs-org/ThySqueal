use super::super::ast::{ComparisonOp, Condition, LogicalOp};
use super::super::error::{SqlError, SqlResult};
use super::expression::evaluate_expression_joined;
use super::{EvalContext, Evaluator};
use crate::storage::Value;

pub fn evaluate_condition_joined(
    executor: &dyn Evaluator,
    cond: &Condition,
    ctx: &EvalContext<'_>,
) -> SqlResult<bool> {
    match cond {
        Condition::Comparison(left, op, right) => {
            let left_val = evaluate_expression_joined(executor, left, ctx)?;
            let right_val = evaluate_expression_joined(executor, right, ctx)?;

            if matches!(left_val, Value::Null) || matches!(right_val, Value::Null) {
                return Ok(false);
            }

            match op {
                ComparisonOp::Eq => Ok(left_val == right_val),
                ComparisonOp::NotEq => Ok(left_val != right_val),
                ComparisonOp::Lt => Ok(left_val < right_val),
                ComparisonOp::Gt => Ok(left_val > right_val),
                ComparisonOp::LtEq => Ok(left_val <= right_val),
                ComparisonOp::GtEq => Ok(left_val >= right_val),
                ComparisonOp::Like => {
                    let l = left_val.as_text().ok_or_else(|| {
                        SqlError::TypeMismatch("LIKE requires text on the left".to_string())
                    })?;
                    let r = right_val.as_text().ok_or_else(|| {
                        SqlError::TypeMismatch("LIKE requires text on the right".to_string())
                    })?;
                    if r.starts_with('%') && r.ends_with('%') {
                        let pat = &r[1..r.len() - 1];
                        Ok(l.contains(pat))
                    } else if let Some(pat) = r.strip_prefix('%') {
                        Ok(l.ends_with(pat))
                    } else if let Some(pat) = r.strip_suffix('%') {
                        Ok(l.starts_with(pat))
                    } else {
                        Ok(l == r)
                    }
                }
            }
        }
        Condition::IsNull(expr) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            Ok(matches!(val, Value::Null))
        }
        Condition::IsNotNull(expr) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            Ok(!matches!(val, Value::Null))
        }
        Condition::InSubquery(expr, subquery) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            let mut combined_outer = ctx.outer_contexts.to_vec();
            combined_outer.extend_from_slice(ctx.contexts);

            let result = futures::executor::block_on(executor.exec_select_internal(
                (**subquery).clone(),
                &combined_outer,
                ctx.params,
                ctx.db_state,
            ))?;
            for row in &result.rows {
                if !row.is_empty() && row[0] == val {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Condition::Logical(left, op, right) => {
            let l = evaluate_condition_joined(executor, left, ctx)?;
            match op {
                LogicalOp::And => {
                    if !l {
                        return Ok(false);
                    }
                    evaluate_condition_joined(executor, right, ctx)
                }
                LogicalOp::Or => {
                    if l {
                        return Ok(true);
                    }
                    evaluate_condition_joined(executor, right, ctx)
                }
            }
        }
        Condition::Not(cond) => Ok(!evaluate_condition_joined(executor, cond, ctx)?),
    }
}
