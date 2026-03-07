mod ast;
mod parser;
mod eval;
pub mod error;

use crate::storage::Database;
use crate::storage::Value;
use ast::SqlStmt;
use parser::parse;
use eval::evaluate_condition;
pub use error::{SqlError, SqlResult};

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: u64,
}

pub struct Executor {
    db: tokio::sync::RwLock<Database>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            db: tokio::sync::RwLock::new(Database::new()),
        }
    }

    pub fn db(&self) -> &tokio::sync::RwLock<Database> {
        &self.db
    }

    pub async fn execute(&self, sql: &str) -> SqlResult<QueryResult> {
        let stmt = parse(sql)?;

        match stmt {
            SqlStmt::CreateTable(ct) => self.exec_create_table(ct).await,
            SqlStmt::DropTable(dt) => self.exec_drop_table(dt).await,
            SqlStmt::Select(s) => self.exec_select(s).await,
            SqlStmt::Insert(i) => self.exec_insert(i).await,
            SqlStmt::Update(u) => self.exec_update(u).await,
            SqlStmt::Delete(d) => self.exec_delete(d).await,
        }
    }

    async fn exec_create_table(&self, stmt: ast::CreateTableStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        db.create_table(stmt.name, stmt.columns)?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }

    async fn exec_drop_table(&self, stmt: ast::DropTableStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        db.drop_table(&stmt.name)?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }

    async fn exec_update(&self, stmt: ast::UpdateStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let mut rows_affected = 0;
        let table_cloned = table.clone();

        for row in table.rows.iter_mut() {
            let matches = if let Some(ref cond) = stmt.where_clause {
                evaluate_condition(cond, &table_cloned, row)?
            } else {
                true
            };

            if matches {
                for (col_name, expr) in &stmt.assignments {
                    let col_idx = table_cloned
                        .column_index(col_name)
                        .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                    let new_val = eval::evaluate_expression(expr, &table_cloned, row)?;
                    row.values[col_idx] = new_val;
                }
                rows_affected += 1;
            }
        }

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected,
        })
    }

    async fn exec_delete(&self, stmt: ast::DeleteStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let mut rows_affected = 0;
        let table_cloned = table.clone();

        let mut i = 0;
        while i < table.rows.len() {
            let matches = if let Some(ref cond) = stmt.where_clause {
                evaluate_condition(cond, &table_cloned, &table.rows[i])?
            } else {
                true
            };

            if matches {
                table.rows.remove(i);
                rows_affected += 1;
            } else {
                i += 1;
            }
        }

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected,
        })
    }

    async fn exec_select(&self, stmt: ast::SelectStmt) -> SqlResult<QueryResult> {
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

        // Handle Aggregates
        if has_aggregates {
            // Very simple global aggregation (no GROUP BY yet)
            let mut row_values = Vec::new();
            for col in &stmt.columns {
                match &col.expr {
                    ast::Expression::FunctionCall(fc) => {
                        let val = self.eval_aggregate(fc, table, &matched_rows)?;
                        row_values.push(val);
                    },
                    ast::Expression::Column(_) | ast::Expression::Literal(_) | ast::Expression::BinaryOp(_, _, _) => {
                        // In standard SQL, selecting non-aggregated columns without GROUP BY is usually an error or returns first row.
                        // For now, let's just evaluate it against the first matched row if it exists.
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
            return Ok(QueryResult {
                columns: result_columns,
                rows: vec![row_values],
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

    fn eval_aggregate(&self, fc: &ast::FunctionCall, table: &crate::storage::Table, rows: &[&crate::storage::Row]) -> SqlResult<Value> {
        match fc.name {
            ast::AggregateType::Count => {
                if fc.args.len() == 1 && matches!(fc.args[0], ast::Expression::Star) {
                    Ok(Value::Int(rows.len() as i64))
                } else {
                    // COUNT(expr) - counts non-null values
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

    async fn exec_insert(&self, stmt: ast::InsertStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        table.insert(stmt.values)?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_table_insert_select() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE t (a INT, b TEXT, c FLOAT)")
            .await
            .unwrap();
        exec.execute("INSERT INTO t (a, b, c) VALUES (1, 'hello', 3.14)")
            .await
            .unwrap();

        let r = exec.execute("SELECT * FROM t").await.unwrap();
        assert_eq!(r.columns, vec!["a", "b", "c"]);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0].as_int(), Some(1));
        assert_eq!(r.rows[0][1].as_text(), Some("hello"));
    }

    #[tokio::test]
    async fn test_select_columns() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE t (a INT, b TEXT, c FLOAT)")
            .await
            .unwrap();
        exec.execute("INSERT INTO t (a, b, c) VALUES (1, 'hello', 3.14)")
            .await
            .unwrap();

        let r = exec.execute("SELECT a, c FROM t").await.unwrap();
        assert_eq!(r.columns, vec!["a", "c"]);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].len(), 2);
        assert_eq!(r.rows[0][0].as_int(), Some(1));
    }

    #[tokio::test]
    async fn test_select_where() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE users (id INT, name TEXT)")
            .await
            .unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
            .await
            .unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (2, 'bob')")
            .await
            .unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (3, 'charlie')")
            .await
            .unwrap();

        let r = exec.execute("SELECT * FROM users WHERE id = 2").await.unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1].as_text(), Some("bob"));

        let r = exec.execute("SELECT name FROM users WHERE id > 1").await.unwrap();
        assert_eq!(r.rows.len(), 2);

        let r = exec.execute("SELECT * FROM users WHERE name = 'alice'").await.unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0].as_int(), Some(1));
    }

    #[tokio::test]
    async fn test_update() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE t (id INT, v TEXT)").await.unwrap();
        exec.execute("INSERT INTO t (id, v) VALUES (1, 'old')").await.unwrap();
        
        let r = exec.execute("UPDATE t SET v = 'new' WHERE id = 1").await.unwrap();
        assert_eq!(r.rows_affected, 1);
        
        let r = exec.execute("SELECT v FROM t WHERE id = 1").await.unwrap();
        assert_eq!(r.rows[0][0].as_text(), Some("new"));
    }

    #[tokio::test]
    async fn test_delete() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE t (id INT)").await.unwrap();
        exec.execute("INSERT INTO t (id) VALUES (1)").await.unwrap();
        exec.execute("INSERT INTO t (id) VALUES (2)").await.unwrap();
        
        let r = exec.execute("DELETE FROM t WHERE id = 1").await.unwrap();
        assert_eq!(r.rows_affected, 1);
        
        let r = exec.execute("SELECT * FROM t").await.unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0].as_int(), Some(2));
    }

    #[tokio::test]
    async fn test_order_by() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE t (id INT)").await.unwrap();
        exec.execute("INSERT INTO t (id) VALUES (3)").await.unwrap();
        exec.execute("INSERT INTO t (id) VALUES (1)").await.unwrap();
        exec.execute("INSERT INTO t (id) VALUES (2)").await.unwrap();

        let r = exec.execute("SELECT * FROM t ORDER BY id ASC").await.unwrap();
        assert_eq!(r.rows[0][0].as_int(), Some(1));
        assert_eq!(r.rows[1][0].as_int(), Some(2));
        assert_eq!(r.rows[2][0].as_int(), Some(3));

        let r = exec.execute("SELECT * FROM t ORDER BY id DESC").await.unwrap();
        assert_eq!(r.rows[0][0].as_int(), Some(3));
        assert_eq!(r.rows[1][0].as_int(), Some(2));
        assert_eq!(r.rows[2][0].as_int(), Some(1));
    }

    #[tokio::test]
    async fn test_limit_offset() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE t (id INT)").await.unwrap();
        for i in 1..=10 {
            exec.execute(&format!("INSERT INTO t (id) VALUES ({})", i))
                .await
                .unwrap();
        }

        let r = exec.execute("SELECT * FROM t ORDER BY id ASC LIMIT 3")
            .await
            .unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0].as_int(), Some(1));
        assert_eq!(r.rows[2][0].as_int(), Some(3));

        let r = exec.execute("SELECT * FROM t ORDER BY id ASC LIMIT 3 OFFSET 2")
            .await
            .unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0].as_int(), Some(3));
        assert_eq!(r.rows[2][0].as_int(), Some(5));
    }

    #[tokio::test]
    async fn test_aggregations_and_aliases() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE sales (id INT, amount FLOAT)")
            .await
            .unwrap();
        exec.execute("INSERT INTO sales (id, amount) VALUES (1, 100.0)")
            .await
            .unwrap();
        exec.execute("INSERT INTO sales (id, amount) VALUES (2, 200.0)")
            .await
            .unwrap();
        exec.execute("INSERT INTO sales (id, amount) VALUES (3, 150.0)")
            .await
            .unwrap();

        let r = exec.execute("SELECT COUNT(*) AS total_count, SUM(amount) AS total_amount FROM sales").await.unwrap();
        assert_eq!(r.columns, vec!["total_count", "total_amount"]);
        assert_eq!(r.rows[0][0].as_int(), Some(3));
        assert_eq!(r.rows[0][1].as_float(), Some(450.0));

        let r = exec.execute("SELECT MIN(amount), MAX(amount), AVG(amount) FROM sales").await.unwrap();
        assert_eq!(r.rows[0][0].as_float(), Some(100.0));
        assert_eq!(r.rows[0][1].as_float(), Some(200.0));
        assert_eq!(r.rows[0][2].as_float(), Some(150.0));
    }

    #[tokio::test]
    async fn test_drop_table() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE x (id INT)").await.unwrap();
        exec.execute("DROP TABLE x").await.unwrap();
        let err = exec.execute("SELECT * FROM x").await.unwrap_err();
        assert!(matches!(err, SqlError::TableNotFound(_)));
    }
}
