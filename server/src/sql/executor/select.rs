use crate::storage::{Value};
use super::super::ast::{self, SelectStmt};
use super::super::error::{SqlError, SqlResult};
use super::super::eval::{self, evaluate_condition};
use super::{QueryResult, Executor};

impl Executor {
    pub(crate) async fn exec_select(&self, stmt: SelectStmt) -> SqlResult<QueryResult> {
        let db = self.db.read().await;
        let table = db
            .get_table(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let is_star = stmt.columns.iter().any(|c| matches!(c.expr, ast::Expression::Star));
        let has_aggregates = stmt.columns.iter().any(|c| matches!(c.expr, ast::Expression::FunctionCall(_)));

        let result_columns: Vec<String> = if is_star {
            table.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            stmt.columns.iter().enumerate().map(|(i, c)| {
                c.alias.clone().unwrap_or_else(|| {
                    match &c.expr {
                        ast::Expression::Column(name) => name.clone(),
                        ast::Expression::FunctionCall(fc) => {
                            let name = format!("{:?}", fc.name).to_uppercase();
                            format!("{}(...)", name)
                        },
                        _ => format!("col_{}", i)
                    }
                })
            }).collect()
        };

        let mut matched_rows = Vec::new();
        for row in &table.rows {
            let matches = if let Some(ref cond) = stmt.where_clause {
                evaluate_condition(cond, table, row)?
            } else {
                true
            };

            if matches {
                matched_rows.push(row);
            }
        }

        // Apply ORDER BY
        if !stmt.order_by.is_empty() {
            let mut err = None;
            matched_rows.sort_by(|a, b| {
                for item in &stmt.order_by {
                    let val_a = match eval::evaluate_expression(&item.expr, table, a) {
                        Ok(v) => v,
                        Err(e) => { err = Some(e); return std::cmp::Ordering::Equal; }
                    };
                    let val_b = match eval::evaluate_expression(&item.expr, table, b) {
                        Ok(v) => v,
                        Err(e) => { err = Some(e); return std::cmp::Ordering::Equal; }
                    };

                    if let Some(ord) = val_a.partial_cmp(&val_b) {
                        if ord != std::cmp::Ordering::Equal {
                            return if item.order == ast::Order::Desc { ord.reverse() } else { ord };
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
            if let Some(e) = err { return Err(e); }
        }

        // Handle Aggregates and Grouping
        if has_aggregates || !stmt.group_by.is_empty() {
            let mut result_rows = Vec::new();

            if stmt.group_by.is_empty() {
                // Global aggregation (single row result)
                let mut row_values = Vec::new();
                for col in &stmt.columns {
                    match &col.expr {
                        ast::Expression::FunctionCall(fc) => {
                            row_values.push(self.eval_aggregate(fc, table, &matched_rows)?);
                        },
                        ast::Expression::Column(_) | ast::Expression::Literal(_) | ast::Expression::BinaryOp(_, _, _) => {
                            if let Some(first_row) = matched_rows.first() {
                                row_values.push(eval::evaluate_expression(&col.expr, table, first_row)?);
                            } else {
                                row_values.push(Value::Null);
                            }
                        },
                        ast::Expression::Star => {
                            return Err(SqlError::Runtime("SELECT * with aggregate is not supported".to_string()));
                        }
                    }
                }
                
                let mut include_row = true;
                if let Some(ref having_cond) = stmt.having {
                    include_row = self.evaluate_having(having_cond, table, &matched_rows)?;
                }

                if include_row {
                    result_rows.push(row_values);
                }
            } else {
                // GROUP BY
                let mut groups: std::collections::HashMap<Vec<Value>, Vec<&crate::storage::Row>> = std::collections::HashMap::new();
                for row in &matched_rows {
                    let mut group_key = Vec::new();
                    for gb_expr in &stmt.group_by {
                        group_key.push(eval::evaluate_expression(gb_expr, table, row)?);
                    }
                    groups.entry(group_key).or_default().push(row);
                }

                for (_key, group_rows) in groups {
                    let include_group = if let Some(ref having_cond) = stmt.having {
                        self.evaluate_having(having_cond, table, &group_rows)?
                    } else {
                        true
                    };

                    if include_group {
                        let mut row_values = Vec::new();
                        for col in &stmt.columns {
                            match &col.expr {
                                ast::Expression::FunctionCall(fc) => {
                                    row_values.push(self.eval_aggregate(fc, table, &group_rows)?);
                                },
                                _ => {
                                    if let Some(first_row) = group_rows.first() {
                                        row_values.push(eval::evaluate_expression(&col.expr, table, first_row)?);
                                    } else {
                                        row_values.push(Value::Null);
                                    }
                                }
                            }
                        }
                        result_rows.push(row_values);
                    }
                }
            }

            return Ok(QueryResult {
                columns: result_columns,
                rows: result_rows,
                rows_affected: 0,
            });
        }

        // Apply LIMIT and OFFSET
        let final_rows = if let Some(limit) = stmt.limit {
            let offset = limit.offset.unwrap_or(0);
            matched_rows.iter().skip(offset).take(limit.count).cloned().collect()
        } else {
            matched_rows
        };

        let rows: Vec<Vec<Value>> = final_rows.iter().map(|row| {
            if is_star {
                row.values.clone()
            } else {
                stmt.columns
                    .iter()
                    .filter_map(|col| {
                        eval::evaluate_expression(&col.expr, table, row).ok()
                    })
                    .collect()
            }
        }).collect();

        Ok(QueryResult {
            columns: result_columns,
            rows,
            rows_affected: 0,
        })
    }

    fn evaluate_having(&self, cond: &ast::Condition, table: &crate::storage::Table, rows: &[&crate::storage::Row]) -> SqlResult<bool> {
        match cond {
            ast::Condition::Comparison(left, op, right) => {
                let left_val = self.evaluate_having_expression(left, table, rows)?;
                let right_val = self.evaluate_having_expression(right, table, rows)?;
                
                match op {
                    ast::ComparisonOp::Eq => Ok(left_val == right_val),
                    ast::ComparisonOp::NotEq => Ok(left_val != right_val),
                    ast::ComparisonOp::Lt => Ok(left_val < right_val),
                    ast::ComparisonOp::Gt => Ok(left_val > right_val),
                    ast::ComparisonOp::LtEq => Ok(left_val <= right_val),
                    ast::ComparisonOp::GtEq => Ok(left_val >= right_val),
                    ast::ComparisonOp::Like => {
                        let l = left_val.as_text().ok_or_else(|| SqlError::TypeMismatch("LIKE requires text".to_string()))?;
                        let r = right_val.as_text().ok_or_else(|| SqlError::TypeMismatch("LIKE requires text".to_string()))?;
                        Ok(l.contains(&r.replace("%", "")))
                    }
                }
            },
            ast::Condition::Logical(left, op, right) => {
                let l = self.evaluate_having(left, table, rows)?;
                match op {
                    ast::LogicalOp::And => Ok(l && self.evaluate_having(right, table, rows)?),
                    ast::LogicalOp::Or => Ok(l || self.evaluate_having(right, table, rows)?),
                }
            },
            ast::Condition::Not(c) => Ok(!self.evaluate_having(c, table, rows)?),
            ast::Condition::IsNull(e) => Ok(self.evaluate_having_expression(e, table, rows)? == Value::Null),
            ast::Condition::IsNotNull(e) => Ok(self.evaluate_having_expression(e, table, rows)? != Value::Null),
        }
    }

    fn evaluate_having_expression(&self, expr: &ast::Expression, table: &crate::storage::Table, rows: &[&crate::storage::Row]) -> SqlResult<Value> {
        match expr {
            ast::Expression::FunctionCall(fc) => self.eval_aggregate(fc, table, rows),
            ast::Expression::Literal(v) => Ok(v.clone()),
            ast::Expression::Column(_) | ast::Expression::BinaryOp(_, _, _) => {
                if let Some(first_row) = rows.first() {
                    eval::evaluate_expression(expr, table, first_row)
                } else {
                    Ok(Value::Null)
                }
            },
            ast::Expression::Star => Err(SqlError::Runtime("Star not allowed in HAVING".to_string())),
        }
    }

    fn eval_aggregate(&self, fc: &ast::FunctionCall, table: &crate::storage::Table, rows: &[&crate::storage::Row]) -> SqlResult<Value> {
        match fc.name {
            ast::AggregateType::Count => {
                if fc.args.len() == 1 && matches!(fc.args[0], ast::Expression::Star) {
                    Ok(Value::Int(rows.len() as i64))
                } else {
                    let mut count = 0;
                    for row in rows {
                        let val = eval::evaluate_expression(&fc.args[0], table, row)?;
                        if val != Value::Null {
                            count += 1;
                        }
                    }
                    Ok(Value::Int(count))
                }
            },
            ast::AggregateType::Sum => {
                let mut sum_f = 0.0;
                let mut sum_i = 0;
                let mut is_float = false;
                for row in rows {
                    let val = eval::evaluate_expression(&fc.args[0], table, row)?;
                    match val {
                        Value::Int(i) => { sum_i += i; sum_f += i as f64; },
                        Value::Float(f) => { sum_f += f; is_float = true; },
                        Value::Null => {},
                        _ => return Err(SqlError::TypeMismatch("SUM requires numeric values".to_string())),
                    }
                }
                if is_float { Ok(Value::Float(sum_f)) } else { Ok(Value::Int(sum_i)) }
            },
            ast::AggregateType::Min => {
                let mut min_val: Option<Value> = None;
                for row in rows {
                    let val = eval::evaluate_expression(&fc.args[0], table, row)?;
                    if val == Value::Null { continue; }
                    if min_val.is_none() || val < min_val.clone().unwrap() {
                        min_val = Some(val);
                    }
                }
                Ok(min_val.unwrap_or(Value::Null))
            },
            ast::AggregateType::Max => {
                let mut max_val: Option<Value> = None;
                for row in rows {
                    let val = eval::evaluate_expression(&fc.args[0], table, row)?;
                    if val == Value::Null { continue; }
                    if max_val.is_none() || val > max_val.clone().unwrap() {
                        max_val = Some(val);
                    }
                }
                Ok(max_val.unwrap_or(Value::Null))
            },
            ast::AggregateType::Avg => {
                let mut sum = 0.0;
                let mut count = 0;
                for row in rows {
                    let val = eval::evaluate_expression(&fc.args[0], table, row)?;
                    match val {
                        Value::Int(i) => { sum += i as f64; count += 1; },
                        Value::Float(f) => { sum += f; count += 1; },
                        Value::Null => {},
                        _ => return Err(SqlError::TypeMismatch("AVG requires numeric values".to_string())),
                    }
                }
                if count == 0 { Ok(Value::Null) } else { Ok(Value::Float(sum / count as f64)) }
            }
        }
    }
}
