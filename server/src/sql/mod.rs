mod ast;
mod parser;

use crate::storage::Database;
use crate::storage::Value;
use ast::SqlStmt;
use parser::parse;

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
            SqlStmt::Update(_) => Err("UPDATE is not implemented yet".to_string()),
            SqlStmt::Delete(_) => Err("DELETE is not implemented yet".to_string()),
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

        let rows: Vec<Vec<Value>> = table.rows.iter().map(|row| {
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
    async fn test_drop_table() {
        let exec = Executor::new();
        exec.execute("CREATE TABLE x (id INT)").await.unwrap();
        exec.execute("DROP TABLE x").await.unwrap();
        let err = exec.execute("SELECT * FROM x").await.unwrap_err();
        assert!(err.contains("Table not found"));
    }
}
