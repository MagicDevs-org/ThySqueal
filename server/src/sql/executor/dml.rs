use super::super::ast::{DeleteStmt, InsertStmt, UpdateStmt};
use super::super::eval::{evaluate_condition, evaluate_expression};
use super::{Executor, QueryResult};
use crate::sql::error::SqlError;
use crate::sql::error::SqlResult;

impl Executor {
    pub(crate) async fn exec_insert(&self, stmt: InsertStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        db.insert(self, &stmt.table, stmt.values)
            .map_err(|e| SqlError::Storage(e.to_string()))?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
        })
    }

    pub(crate) async fn exec_update(&self, stmt: UpdateStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let mut rows_affected = 0;
        let mut updated_rows = Vec::new();

        for row in &table.rows {
            if let Some(ref where_clause) = stmt.where_clause {
                if evaluate_condition(self, where_clause, table, row)? {
                    let mut new_values = row.values.clone();
                    for (col_name, expr) in &stmt.assignments {
                        let col_idx = table
                            .column_index(col_name)
                            .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                        new_values[col_idx] = evaluate_expression(self, expr, table, row)?;
                    }
                    updated_rows.push((row.id.clone(), new_values));
                    rows_affected += 1;
                }
            } else {
                let mut new_values = row.values.clone();
                for (col_name, expr) in &stmt.assignments {
                    let col_idx = table
                        .column_index(col_name)
                        .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                    new_values[col_idx] = evaluate_expression(self, expr, table, row)?;
                }
                updated_rows.push((row.id.clone(), new_values));
                rows_affected += 1;
            }
        }

        for (id, values) in updated_rows {
            table.update(self, &id, values)?;
        }

        db.save().map_err(|e| SqlError::Storage(e.to_string()))?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected,
        })
    }

    pub(crate) async fn exec_delete(&self, stmt: DeleteStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let mut rows_affected = 0;
        let mut ids_to_delete = Vec::new();

        for row in &table.rows {
            if let Some(ref where_clause) = stmt.where_clause {
                if evaluate_condition(self, where_clause, table, row)? {
                    ids_to_delete.push(row.id.clone());
                    rows_affected += 1;
                }
            } else {
                ids_to_delete.push(row.id.clone());
                rows_affected += 1;
            }
        }

        for id in ids_to_delete {
            table.delete(self, &id)?;
        }

        db.save().map_err(|e| SqlError::Storage(e.to_string()))?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected,
        })
    }
}
