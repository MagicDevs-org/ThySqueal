use super::super::ast::{CreateTableStmt, DropTableStmt};
use super::super::error::SqlResult;
use super::{QueryResult, Executor};

impl Executor {
    pub(crate) async fn exec_create_table(&self, stmt: CreateTableStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        db.create_table(stmt.name, stmt.columns)?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }

    pub(crate) async fn exec_drop_table(&self, stmt: DropTableStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        db.drop_table(&stmt.name)?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }
}
