use crate::storage::{Row, Table, Value};
use super::ast::{BinaryOp, ComparisonOp, Condition, Expression, LogicalOp};
use super::error::{SqlError, SqlResult};

pub fn evaluate_condition(cond: &Condition, table: &Table, row: &Row) -> SqlResult<bool> {
    match cond {
        Condition::Comparison(left, op, right) => {
            let left_val = evaluate_expression(left, table, row)?;
            let right_val = evaluate_expression(right, table, row)?;
            
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
                    // Very simple LIKE: only handles % as prefix/suffix or exact match
                    if r.starts_with('%') && r.ends_with('%') {
                        let pat = &r[1..r.len()-1];
                        Ok(l.contains(pat))
                    } else if r.starts_with('%') {
                        let pat = &r[1..];
                        Ok(l.ends_with(pat))
                    } else if r.ends_with('%') {
                        let pat = &r[..r.len()-1];
                        Ok(l.starts_with(pat))
                    } else {
                        Ok(l == r)
                    }
                }
            }
        }
        Condition::IsNull(expr) => {
            let val = evaluate_expression(expr, table, row)?;
            Ok(matches!(val, Value::Null))
        }
        Condition::IsNotNull(expr) => {
            let val = evaluate_expression(expr, table, row)?;
            Ok(!matches!(val, Value::Null))
        }
        Condition::Logical(left, op, right) => {
            let l = evaluate_condition(left, table, row)?;
            match op {
                LogicalOp::And => {
                    if !l { return Ok(false); }
                    evaluate_condition(right, table, row)
                }
                LogicalOp::Or => {
                    if l { return Ok(true); }
                    evaluate_condition(right, table, row)
                }
            }
        }
        Condition::Not(cond) => {
            Ok(!evaluate_condition(cond, table, row)?)
        }
    }
}

pub fn evaluate_expression(expr: &Expression, table: &Table, row: &Row) -> SqlResult<Value> {
    match expr {
        Expression::Literal(v) => Ok(v.clone()),
        Expression::Column(name) => {
            let idx = table.column_index(name)
                .ok_or_else(|| SqlError::ColumnNotFound(name.clone()))?;
            row.values.get(idx).cloned()
                .ok_or_else(|| SqlError::Runtime(format!("Value not found for column index: {}", idx)))
        }
        Expression::BinaryOp(left, op, right) => {
            let l = evaluate_expression(left, table, row)?;
            let r = evaluate_expression(right, table, row)?;
            
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
                // Mix of Int and Float -> promote to Float
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
    }
}
