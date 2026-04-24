use super::expression::evaluate_expression_joined;
use super::{EvalContext, Evaluator};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::{ComparisonOp, Condition, IsOp};
use crate::storage::Value;

fn coerce_comparison_values(a: &Value, b: &Value) -> (Value, Value) {
    match (a, b) {
        (Value::Text(s), Value::Int(_)) => {
            if let Ok(parsed) = s.parse::<i64>() {
                (Value::Int(parsed), b.clone())
            } else {
                (a.clone(), b.clone())
            }
        }
        (Value::Int(_), Value::Text(s)) => {
            if let Ok(parsed) = s.parse::<i64>() {
                (a.clone(), Value::Int(parsed))
            } else {
                (a.clone(), b.clone())
            }
        }
        (Value::Text(a_str), Value::Text(b_str)) => {
            if let (Ok(ai), Ok(bi)) = (a_str.parse::<i64>(), b_str.parse::<i64>()) {
                (Value::Int(ai), Value::Int(bi))
            } else if let (Ok(af), Ok(bf)) = (a_str.parse::<f64>(), b_str.parse::<f64>()) {
                (Value::Float(af), Value::Float(bf))
            } else {
                (a.clone(), b.clone())
            }
        }
        (Value::Int(i), Value::Float(_)) => (Value::Float(*i as f64), b.clone()),
        (Value::Float(_), Value::Int(i)) => (a.clone(), Value::Float(*i as f64)),
        _ => (a.clone(), b.clone()),
    }
}

pub fn evaluate_condition_joined(
    executor: &dyn Evaluator,
    cond: &Condition,
    ctx: &EvalContext<'_>,
) -> ExecResult<bool> {
    match cond {
        Condition::And(left, right) => {
            let l = evaluate_condition_joined(executor, left, ctx)?;
            if !l {
                return Ok(false);
            }
            evaluate_condition_joined(executor, right, ctx)
        }
        Condition::Or(left, right) => {
            let l = evaluate_condition_joined(executor, left, ctx)?;
            if l {
                return Ok(true);
            }
            evaluate_condition_joined(executor, right, ctx)
        }
        Condition::Not(c) => Ok(!evaluate_condition_joined(executor, c, ctx)?),
        Condition::Comparison(left, op, right) => {
            let left_val = evaluate_expression_joined(executor, left, ctx)?;
            let right_val = evaluate_expression_joined(executor, right, ctx)?;

            if matches!(left_val, Value::Null) || matches!(right_val, Value::Null) {
                return Ok(false);
            }

            let (left_val, right_val) = coerce_comparison_values(&left_val, &right_val);

            match op {
                ComparisonOp::Eq => Ok(left_val == right_val),
                ComparisonOp::Neq => Ok(left_val != right_val),
                ComparisonOp::Lt => Ok(left_val < right_val),
                ComparisonOp::Gt => Ok(left_val > right_val),
                ComparisonOp::Lte => Ok(left_val <= right_val),
                ComparisonOp::Gte => Ok(left_val >= right_val),
            }
        }
        Condition::In(expr, values) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            for v_expr in values {
                let v = evaluate_expression_joined(executor, v_expr, ctx)?;
                if v == val {
                    return Ok(true);
                }
            }
            Ok(false)
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
        Condition::Exists(subquery) => {
            let mut combined_outer = ctx.outer_contexts.to_vec();
            combined_outer.extend_from_slice(ctx.contexts);

            let result = futures::executor::block_on(executor.exec_select_internal(
                (**subquery).clone(),
                &combined_outer,
                ctx.params,
                ctx.db_state,
            ))?;
            Ok(!result.rows.is_empty())
        }
        Condition::Between(expr, low, high) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            let l = evaluate_expression_joined(executor, low, ctx)?;
            let h = evaluate_expression_joined(executor, high, ctx)?;
            Ok(val >= l && val <= h)
        }
        Condition::Is(expr, op) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            match op {
                IsOp::Null => Ok(matches!(val, Value::Null)),
                IsOp::NotNull => Ok(!matches!(val, Value::Null)),
                IsOp::True => Ok(matches!(val, Value::Bool(true))),
                IsOp::False => Ok(matches!(val, Value::Bool(false))),
            }
        }
        Condition::Like(expr, pattern) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            let l = val.as_text().ok_or_else(|| {
                ExecError::TypeMismatch("LIKE requires text on the left".to_string())
            })?;

            if pattern.starts_with('%') && pattern.ends_with('%') {
                let pat = &pattern[1..pattern.len() - 1];
                Ok(l.contains(pat))
            } else if let Some(pat) = pattern.strip_prefix('%') {
                Ok(l.ends_with(pat))
            } else if let Some(pat) = pattern.strip_suffix('%') {
                Ok(l.starts_with(pat))
            } else {
                Ok(l == *pattern)
            }
        }
        Condition::FullTextSearch(_field, _query) => {
            // This is usually handled at the table level using indexes,
            // but for manual evaluation (if needed):
            Err(ExecError::Runtime(
                "FullTextSearch must be handled by the storage engine".to_string(),
            ))
        }
    }
}
