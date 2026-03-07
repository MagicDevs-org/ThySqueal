use super::super::ast::{CreateIndexStmt, CreateTableStmt, DropTableStmt, IndexType};
use super::super::error::{SqlError, SqlResult};
use super::{Executor, QueryResult};

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

    pub(crate) async fn exec_create_index(&self, stmt: CreateIndexStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let use_hash = stmt.index_type == IndexType::Hash;
        tracing::info!("Creating index {} on {} (unique={})", stmt.name, stmt.table, stmt.unique);
        table.create_index(self, stmt.name, stmt.expressions, stmt.unique, use_hash)?;
        db.save().map_err(|e| SqlError::Storage(e.to_string()))?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }
}
