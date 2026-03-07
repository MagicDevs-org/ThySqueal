use super::super::ast::{CreateIndexStmt, CreateTableStmt, DropTableStmt, IndexType};
use super::super::error::{SqlError, SqlResult};
use super::{Executor, QueryResult};
use crate::storage::Table;

impl Executor {
    pub(crate) async fn exec_create_table(&self, stmt: CreateTableStmt, tx_id: Option<&str>) -> SqlResult<QueryResult> {
        if let Some(id) = tx_id {
            self.mutate_state(Some(id), |state| {
                if state.get_table(&stmt.name).is_some() {
                    return Err(SqlError::Storage(format!("Table {} already exists", stmt.name)));
                }
                state.tables.insert(stmt.name.clone(), Table::new(stmt.name, stmt.columns));
                Ok(())
            }).await?;
        } else {
            let mut db = self.db.write().await;
            db.create_table(stmt.name, stmt.columns).map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_drop_table(&self, stmt: DropTableStmt, tx_id: Option<&str>) -> SqlResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            state.tables.remove(&stmt.name).ok_or_else(|| SqlError::TableNotFound(stmt.name.clone()))?;
            Ok(())
        }).await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_create_index(&self, stmt: CreateIndexStmt, tx_id: Option<&str>) -> SqlResult<QueryResult> {
        let use_hash = stmt.index_type == IndexType::Hash;
        tracing::info!("Creating index {} on {} (unique={})", stmt.name, stmt.table, stmt.unique);

        self.mutate_state(tx_id, |state| {
            let db_state = state.clone();
            let table = state.get_table_mut(&stmt.table).ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
            table.create_index(self, stmt.name, stmt.expressions, stmt.unique, use_hash, stmt.where_clause, &db_state)
                .map_err(|e| SqlError::Storage(e.to_string()))?;
            Ok(())
        }).await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }
}
