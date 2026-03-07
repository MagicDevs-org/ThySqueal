use super::super::ast::{InsertStmt, UpdateStmt, DeleteStmt};
use super::super::error::{SqlError, SqlResult};
use super::super::eval::{evaluate_condition_joined, evaluate_expression_joined};
use super::{QueryResult, Executor};

impl Executor {
    pub(crate) async fn exec_insert(&self, stmt: InsertStmt) -> SqlResult<QueryResult> {
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

    pub(crate) async fn exec_update(&self, stmt: UpdateStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let mut rows_affected = 0;
        let table_cloned = table.clone();

        for row in table.rows.iter_mut() {
            let matches = if let Some(ref cond) = stmt.where_clause {
                evaluate_condition_joined(cond, &[(&table_cloned, row)])?
            } else {
                true
            };

            if matches {
                for (col_name, expr) in &stmt.assignments {
                    let col_idx = table_cloned
                        .column_index(col_name)
                        .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                    let new_val = evaluate_expression_joined(expr, &[(&table_cloned, row)])?;
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

    pub(crate) async fn exec_delete(&self, stmt: DeleteStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let mut rows_affected = 0;
        let table_cloned = table.clone();

        let mut i = 0;
        while i < table.rows.len() {
            let matches = if let Some(ref cond) = stmt.where_clause {
                evaluate_condition_joined(cond, &[(&table_cloned, &table.rows[i])])?
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
}
