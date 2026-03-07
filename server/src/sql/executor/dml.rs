use super::super::ast::{DeleteStmt, InsertStmt, UpdateStmt};
use super::super::eval::{evaluate_condition, evaluate_expression};
use super::{Executor, QueryResult};
use crate::sql::error::SqlError;
use crate::sql::error::SqlResult;

impl Executor {
    pub(crate) async fn exec_insert(&self, stmt: InsertStmt, tx_id: Option<&str>) -> SqlResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            let db_state = state.clone(); // Clone for evaluation (could be optimized)
            state.get_table_mut(&stmt.table)
                .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?
                .insert(self, stmt.values, &db_state)
                .map_err(|e| SqlError::Storage(e.to_string()))?;
            Ok(())
        }).await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_update(&self, stmt: UpdateStmt, tx_id: Option<&str>) -> SqlResult<QueryResult> {
        let rows_affected = self.mutate_state(tx_id, |state| {
            let mut updated_rows = Vec::new();
            let mut count = 0;
            let db_state = state.clone();
            {
                let table = state.get_table(&stmt.table).ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                for row in &table.rows {
                    let matches = if let Some(ref where_clause) = stmt.where_clause {
                        evaluate_condition(self, where_clause, table, row, &db_state)?
                    } else {
                        true
                    };

                    if matches {
                        let mut new_values = row.values.clone();
                        for (col_name, expr) in &stmt.assignments {
                            let col_idx = table.column_index(col_name).ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                            new_values[col_idx] = evaluate_expression(self, expr, table, row, &db_state)?;
                        }
                        updated_rows.push((row.id.clone(), new_values));
                        count += 1;
                    }
                }
            }

            let table = state.get_table_mut(&stmt.table).unwrap();
            for (row_id, values) in updated_rows {
                table.update(self, &row_id, values, &db_state).map_err(|e| SqlError::Storage(e.to_string()))?;
            }
            Ok(count)
        }).await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_delete(&self, stmt: DeleteStmt, tx_id: Option<&str>) -> SqlResult<QueryResult> {
        let rows_affected = self.mutate_state(tx_id, |state| {
            let mut ids_to_delete = Vec::new();
            let mut count = 0;
            let db_state = state.clone();
            {
                let table = state.get_table(&stmt.table).ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                for row in &table.rows {
                    let matches = if let Some(ref where_clause) = stmt.where_clause {
                        evaluate_condition(self, where_clause, table, row, &db_state)?
                    } else {
                        true
                    };

                    if matches {
                        ids_to_delete.push(row.id.clone());
                        count += 1;
                    }
                }
            }

            let table = state.get_table_mut(&stmt.table).unwrap();
            for row_id in ids_to_delete {
                table.delete(self, &row_id, &db_state).map_err(|e| SqlError::Storage(e.to_string()))?;
            }
            Ok(count)
        }).await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }
}
