mod ast;
mod parser;
mod eval;

use crate::storage::Database;
use crate::storage::Value;
use ast::SqlStmt;
use parser::parse;
use eval::evaluate_condition;

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

    pub async fn execute(&self, sql: &str) -> Result<QueryResult, String> {
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

    async fn exec_create_table(&self, stmt: ast::CreateTableStmt) -> Result<QueryResult, String> {
        let mut db = self.db.write().await;
        db.create_table(stmt.name, stmt.columns).map_err(|e| e.to_string())?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }

    async fn exec_drop_table(&self, stmt: ast::DropTableStmt) -> Result<QueryResult, String> {
        let mut db = self.db.write().await;
        db.drop_table(&stmt.name).map_err(|e| e.to_string())?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }

    async fn exec_update(&self, stmt: ast::UpdateStmt) -> Result<QueryResult, String> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| format!("Table not found: {}", stmt.table))?;

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
                        .ok_or_else(|| format!("Column not found: {}", col_name))?;
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

    async fn exec_delete(&self, stmt: ast::DeleteStmt) -> Result<QueryResult, String> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| format!("Table not found: {}", stmt.table))?;

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

    async fn exec_select(&self, stmt: ast::SelectStmt) -> Result<QueryResult, String> {
        let db = self.db.read().await;
        let table = db
            .get_table(&stmt.table)
            .ok_or_else(|| format!("Table not found: {}", stmt.table))?;

        let result_columns = if stmt.columns.iter().any(|c| c == "*") {
            table.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            stmt.columns.clone()
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

        // Apply LIMIT and OFFSET
        let final_rows = if let Some(limit) = stmt.limit {
            let offset = limit.offset.unwrap_or(0);
            matched_rows.iter().skip(offset).take(limit.count).cloned().collect()
        } else {
            matched_rows
        };

        let rows: Vec<Vec<Value>> = final_rows.iter().map(|row| {
            if stmt.columns.iter().any(|c| c == "*") {
                row.values.clone()
            } else {
                stmt.columns
                    .iter()
                    .filter_map(|col| {
                        table
                            .column_index(col)
                            .and_then(|idx| row.values.get(idx).cloned())
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

    async fn exec_insert(&self, stmt: ast::InsertStmt) -> Result<QueryResult, String> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| format!("Table not found: {}", stmt.table))?;

        table.insert(stmt.values).map_err(|e| e.to_string())?;

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

        exec.execute("CREATE TABLE users (id INT, name TEXT)")
            .await
            .unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
            .await
            .unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (2, 'bob')")
            .await
            .unwrap();

        let r = exec.execute("SELECT * FROM users").await.unwrap();
        assert_eq!(r.columns, vec!["id", "name"]);
        assert_eq!(r.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_select_columns() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE t (a INT, b TEXT, c BOOL)")
            .await
            .unwrap();
        exec.execute("INSERT INTO t (a, b, c) VALUES (1, 'x', true)")
            .await
            .unwrap();

        let r = exec.execute("SELECT a, c FROM t").await.unwrap();
        assert_eq!(r.columns, vec!["a", "c"]);
        assert_eq!(r.rows.len(), 1);
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
    async fn test_drop_table() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE x (id INT)").await.unwrap();
        exec.execute("DROP TABLE x").await.unwrap();
        let err = exec.execute("SELECT * FROM x").await.unwrap_err();
        assert!(err.contains("Table not found"));
    }
}
