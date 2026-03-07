use crate::storage::{Row, Table, Value, DatabaseState};
use super::ast::{BinaryOp, ComparisonOp, Condition, Expression, LogicalOp, ScalarFuncType};
use super::error::{SqlError, SqlResult};
use super::executor::Executor;

#[allow(dead_code)]
pub fn evaluate_condition(executor: &Executor, cond: &Condition, table: &Table, row: &Row, db_state: &DatabaseState) -> SqlResult<bool> {
    evaluate_condition_joined(executor, cond, &[(table, row)], &[], db_state)
}

pub fn evaluate_condition_joined(
    executor: &Executor,
    cond: &Condition,
    contexts: &[(&Table, &Row)],
    outer_contexts: &[(&Table, &Row)],
    db_state: &DatabaseState,
) -> SqlResult<bool> {
    match cond {
        Condition::Comparison(left, op, right) => {
            let left_val = evaluate_expression_joined(executor, left, contexts, outer_contexts, db_state)?;
            let right_val = evaluate_expression_joined(executor, right, contexts, outer_contexts, db_state)?;
            
            match op {
                ComparisonOp::Eq => Ok(left_val == right_val),
                ComparisonOp::NotEq => Ok(left_val != right_val),
                ComparisonOp::Lt => Ok(left_val < right_val),
                ComparisonOp::Gt => Ok(left_val > right_val),
                ComparisonOp::LtEq => Ok(left_val <= right_val),
                ComparisonOp::GtEq => Ok(left_val >= right_val),
                ComparisonOp::Like => {
                    let l = left_val.as_text().ok_or_else(|| SqlError::TypeMismatch("LIKE requires text on the left".to_string()))?;
                    let r = right_val.as_text().ok_or_else(|| SqlError::TypeMismatch("LIKE requires text on the right".to_string()))?;
                    if r.starts_with('%') && r.ends_with('%') {
                        let pat = &r[1..r.len()-1];
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
            let val = evaluate_expression_joined(executor, expr, contexts, outer_contexts, db_state)?;
            Ok(matches!(val, Value::Null))
        }
        Condition::IsNotNull(expr) => {
            let val = evaluate_expression_joined(executor, expr, contexts, outer_contexts, db_state)?;
            Ok(!matches!(val, Value::Null))
        }
        Condition::InSubquery(expr, subquery) => {
            let val = evaluate_expression_joined(executor, expr, contexts, outer_contexts, db_state)?;
            let mut combined_outer = outer_contexts.to_vec();
            combined_outer.extend_from_slice(contexts);
            
            let result = futures::executor::block_on(executor.exec_select_internal((**subquery).clone(), &combined_outer, db_state))?;
            for row in result.rows {
                if !row.is_empty() && row[0] == val {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Condition::Logical(left, op, right) => {
            let l = evaluate_condition_joined(executor, left, contexts, outer_contexts, db_state)?;
            match op {
                LogicalOp::And => {
                    if !l { return Ok(false); }
                    evaluate_condition_joined(executor, right, contexts, outer_contexts, db_state)
                }
                LogicalOp::Or => {
                    if l { return Ok(true); }
                    evaluate_condition_joined(executor, right, contexts, outer_contexts, db_state)
                }
            }
        }
        Condition::Not(cond) => {
            Ok(!evaluate_condition_joined(executor, cond, contexts, outer_contexts, db_state)?)
        }
    }
}

#[allow(dead_code)]
pub fn evaluate_expression(executor: &Executor, expr: &Expression, table: &Table, row: &Row, db_state: &DatabaseState) -> SqlResult<Value> {
    evaluate_expression_joined(executor, expr, &[(table, row)], &[], db_state)
}

pub fn evaluate_expression_joined(
    executor: &Executor,
    expr: &Expression,
    contexts: &[(&Table, &Row)],
    outer_contexts: &[(&Table, &Row)],
    db_state: &DatabaseState,
) -> SqlResult<Value> {
    match expr {
        Expression::Literal(v) => Ok(v.clone()),
        Expression::Column(name) => {
            if let Ok(val) = resolve_column(name, contexts) {
                return Ok(val);
            }
            if let Ok(val) = resolve_column(name, outer_contexts) {
                return Ok(val);
            }
            
            Err(SqlError::ColumnNotFound(name.clone()))
        }
        Expression::Subquery(subquery) => {
            let mut combined_outer = outer_contexts.to_vec();
            combined_outer.extend_from_slice(contexts);

            let result = futures::executor::block_on(executor.exec_select_internal((**subquery).clone(), &combined_outer, db_state))?;
            if result.rows.is_empty() {
                Ok(Value::Null)
            } else if result.rows.len() > 1 {
                Err(SqlError::Runtime("Subquery returned more than one row".to_string()))
            } else if result.rows[0].is_empty() {
                Ok(Value::Null)
            } else {
                Ok(result.rows[0][0].clone())
            }
        }
        Expression::BinaryOp(left, op, right) => {
            let l = evaluate_expression_joined(executor, left, contexts, outer_contexts, db_state)?;
            let r = evaluate_expression_joined(executor, right, contexts, outer_contexts, db_state)?;
            
            match (l, r) {
                (Value::Int(a), Value::Int(b)) => {
                    match op {
                        BinaryOp::Add => Ok(Value::Int(a + b)),
                        BinaryOp::Sub => Ok(Value::Int(a - b)),
                        BinaryOp::Mul => Ok(Value::Int(a * b)),
                        BinaryOp::Div => {
                            if b == 0 { return Err(SqlError::Runtime("Division by zero".to_string())); }
                            Ok(Value::Int(a / b))
                        }
                    }
                }
                (Value::Float(a), Value::Float(b)) => {
                    match op {
                        BinaryOp::Add => Ok(Value::Float(a + b)),
                        BinaryOp::Sub => Ok(Value::Float(a - b)),
                        BinaryOp::Mul => Ok(Value::Float(a * b)),
                        BinaryOp::Div => Ok(Value::Float(a / b)),
                    }
                }
                (Value::Int(a), Value::Float(b)) => {
                    let a = a as f64;
                    match op {
                        BinaryOp::Add => Ok(Value::Float(a + b)),
                        BinaryOp::Sub => Ok(Value::Float(a - b)),
                        BinaryOp::Mul => Ok(Value::Float(a * b)),
                        BinaryOp::Div => Ok(Value::Float(a / b)),
                    }
                }
                (Value::Float(a), Value::Int(b)) => {
                    let b = b as f64;
                    match op {
                        BinaryOp::Add => Ok(Value::Float(a + b)),
                        BinaryOp::Sub => Ok(Value::Float(a - b)),
                        BinaryOp::Mul => Ok(Value::Float(a * b)),
                        BinaryOp::Div => Ok(Value::Float(a / b)),
                    }
                }
                _ => Err(SqlError::TypeMismatch("Unsupported types for binary operation".to_string())),
            }
        }
        Expression::ScalarFunc(sf) => {
            let val = evaluate_expression_joined(executor, &sf.arg, contexts, outer_contexts, db_state)?;
            match sf.name {
                ScalarFuncType::Lower => {
                    let s = val.as_text().ok_or_else(|| SqlError::TypeMismatch("LOWER requires text".to_string()))?;
                    Ok(Value::Text(s.to_lowercase()))
                }
                ScalarFuncType::Upper => {
                    let s = val.as_text().ok_or_else(|| SqlError::TypeMismatch("UPPER requires text".to_string()))?;
                    Ok(Value::Text(s.to_uppercase()))
                }
                ScalarFuncType::Length => {
                    let s = val.as_text().ok_or_else(|| SqlError::TypeMismatch("LENGTH requires text".to_string()))?;
                    Ok(Value::Int(s.len() as i64))
                }
                ScalarFuncType::Abs => {
                    match val {
                        Value::Int(i) => Ok(Value::Int(i.abs())),
                        Value::Float(f) => Ok(Value::Float(f.abs())),
                        _ => Err(SqlError::TypeMismatch("ABS requires numeric value".to_string())),
                    }
                }
            }
        }
        Expression::FunctionCall(_) => {
            Err(SqlError::Runtime("Aggregate functions must be evaluated at the top level".to_string()))
        }
        Expression::Star => {
            Err(SqlError::Runtime("Star expression must be evaluated at the top level".to_string()))
        }
    }
}

pub fn resolve_column(name: &str, contexts: &[(&Table, &Row)]) -> SqlResult<Value> {
    if name.contains('.') {
        let parts: Vec<&str> = name.split('.').collect();
        // Case 1: table.column[.json_path]
        for (table, row) in contexts {
            if table.name == parts[0] {
                if let Some(idx) = table.column_index(parts[1]) {
                    let mut current_val = row.values.get(idx).cloned()
                        .ok_or_else(|| SqlError::Runtime(format!("Value not found for column index: {}", idx)))?;
                    
                    // JSON path traversal
                    for part in &parts[2..] {
                        current_val = match current_val {
                            Value::Json(v) => v.get(*part).map(|inner| Value::from_json(inner.clone())).unwrap_or(Value::Null),
                            _ => Value::Null,
                        };
                        if current_val == Value::Null { break; }
                    }
                    return Ok(current_val);
                }
            }
        }
        
        // Case 2: column.json_path (no table prefix)
        for (table, row) in contexts {
            if let Some(idx) = table.column_index(parts[0]) {
                let mut current_val = row.values.get(idx).cloned()
                    .ok_or_else(|| SqlError::Runtime(format!("Value not found for column index: {}", idx)))?;
                
                for part in &parts[1..] {
                    current_val = match current_val {
                        Value::Json(v) => v.get(*part).map(|inner| Value::from_json(inner.clone())).unwrap_or(Value::Null),
                        _ => Value::Null,
                    };
                    if current_val == Value::Null { break; }
                }
                return Ok(current_val);
            }
        }
    } else {
        // Simple column
        for (table, row) in contexts {
            if let Some(idx) = table.column_index(name) {
                return row.values.get(idx).cloned()
                    .ok_or_else(|| SqlError::Runtime(format!("Value not found for column index: {}", idx)));
            }
        }
    }
    Err(SqlError::ColumnNotFound(name.to_string()))
}
