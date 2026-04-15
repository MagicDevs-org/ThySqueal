use super::super::{Executor, QueryResult};
use crate::squeal::eval::{EvalContext, Evaluator, evaluate_expression_joined};
use crate::squeal::exec::Session;
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::{Insert, InsertMode};
use crate::storage::{Value, WalRecord};

impl Executor {
    pub(crate) async fn exec_insert(
        &self,
        stmt: Insert,
        params: &[Value],
        session: Session,
    ) -> ExecResult<QueryResult> {
        let table_name = stmt.table.clone();
        let tx_id = session.transaction_id.as_deref();

        let db = self.db.read().await;
        let state = if let Some(id) = tx_id {
            self.transactions
                .get(id)
                .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?
                .clone()
        } else {
            db.state().clone()
        };

        let table = state
            .get_table(&table_name)
            .ok_or_else(|| ExecError::TableNotFound(table_name.clone()))?;

        let column_count = if let Some(ref cols) = stmt.columns {
            cols.len()
        } else {
            table.columns().len()
        };

        if stmt.values.len() != column_count {
            return Err(ExecError::TypeMismatch(format!(
                "Value count mismatch: expected {}, got {}",
                column_count,
                stmt.values.len()
            )));
        }

        let eval_ctx = EvalContext::new(&[], params, &[], &state).with_session(&session);

        // Map expressions to table columns
        let mut mapped_values = if let Some(ref col_names) = stmt.columns {
            // Initialize with NULLs
            let mut vals = vec![Value::Null; table.columns().len()];
            for (i, name) in col_names.iter().enumerate() {
                let col_idx = table
                    .column_index(name)
                    .ok_or_else(|| ExecError::ColumnNotFound(format!("{}.{}", table_name, name)))?;

                let mut val = evaluate_expression_joined(self, &stmt.values[i], &eval_ctx)?;
                let target_type = &table.columns()[col_idx].data_type;
                val = val.cast(target_type).map_err(|e| {
                    ExecError::TypeMismatch(format!(
                        "Error casting value for column '{}': {}",
                        name, e
                    ))
                })?;
                vals[col_idx] = val;
            }
            vals
        } else {
            // Position-based mapping
            let mut vals = Vec::new();
            for (i, expr) in stmt.values.iter().enumerate() {
                let mut val = evaluate_expression_joined(self, expr, &eval_ctx)?;
                let target_type = &table.columns()[i].data_type;
                val = val.cast(target_type).map_err(|e| {
                    ExecError::TypeMismatch(format!(
                        "Error casting value for column '{}': {}",
                        table.columns()[i].name,
                        e
                    ))
                })?;
                vals.push(val);
            }
            vals
        };

        drop(db); // Release read lock before mutation

        // Generate auto-increment values
        self.mutate_state(tx_id, |state| {
            let table = state
                .get_table_mut(&table_name)
                .ok_or_else(|| ExecError::TableNotFound(table_name.clone()))?;

            let mut to_generate = Vec::new();
            for (i, col) in table.columns().iter().enumerate() {
                if col.is_auto_increment && matches!(&mapped_values[i], Value::Null) {
                    to_generate.push(i);
                }
            }

            for i in to_generate {
                if let Some(next_val) = table.generate_auto_inc(i) {
                    mapped_values[i] = Value::Int(next_val as i64);
                }
            }
            Ok(())
        })
        .await?;

        // Handle REPLACE mode - delete existing row with same primary key first
        let mut rows_affected = 1;
        if matches!(stmt.mode, InsertMode::Replace) {
            let pk_columns: Vec<String> = table.schema.primary_key.clone().unwrap_or_default();

            if !pk_columns.is_empty() {
                let mut delete_condition = String::new();
                delete_condition.push_str("WHERE ");
                for (i, pk_col) in pk_columns.iter().enumerate() {
                    if i > 0 {
                        delete_condition.push_str(" AND ");
                    }
                    let pk_idx = table.columns().iter().position(|c| c.name == *pk_col);
                    if let Some(idx) = pk_idx {
                        let val = &mapped_values[idx];
                        delete_condition.push_str(&format!(
                            "{} = {}",
                            pk_col,
                            match val {
                                Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
                                Value::Int(n) => n.to_string(),
                                _ => "NULL".to_string(),
                            }
                        ));
                    }
                }

                let delete_query = format!("DELETE FROM {} {}", table_name, delete_condition);
                let delete_session =
                    Session::new(Some(session.username.clone()), session.database.clone());

                match self.execute(&delete_query, vec![], delete_session).await {
                    Ok(result) => {
                        rows_affected += result.rows_affected;
                    }
                    Err(_) => {}
                }
            }
        }

        // Handle ON DUPLICATE KEY UPDATE
        if let Some(ref updates) = stmt.on_duplicate_update {
            if !updates.is_empty() {
                // First try to insert - if it succeeds, rows_affected = 1
                // If duplicate key, MySQL returns rows_affected = 2 (1 delete + 1 update)
                // We'll check if we need to run UPDATE after insert
                let db_check = self.db.read().await;
                let table_schema = db_check.state().get_table(&table_name);
                let pk_cols: Vec<String> = table_schema
                    .and_then(|t| t.schema.primary_key.clone())
                    .unwrap_or_default();
                let columns: Vec<String> = table_schema
                    .map(|t| t.columns().iter().map(|c| c.name.clone()).collect())
                    .unwrap_or_default();

                if !pk_cols.is_empty() {
                    // Try to find if there's a duplicate by checking if insert actually added a row
                    let mut has_duplicate = false;
                    for (col_name, _) in updates {
                        if columns.iter().position(|c| c == col_name).is_some() {
                            has_duplicate = true;
                            break;
                        }
                    }

                    drop(db_check);

                    if has_duplicate {
                        // Build UPDATE query from on_duplicate_update
                        let _set_clause: Vec<String> = updates
                            .iter()
                            .map(|(col, _expr)| format!("{} = ", col))
                            .collect();

                        let mut update_condition = String::new();
                        update_condition.push_str("WHERE ");
                        for (i, pk_col) in pk_cols.iter().enumerate() {
                            if i > 0 {
                                update_condition.push_str(" AND ");
                            }
                            let pk_val = mapped_values.get(i).unwrap_or(&Value::Null);
                            update_condition.push_str(&format!(
                                "{} = {}",
                                pk_col,
                                match pk_val {
                                    Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
                                    Value::Int(n) => n.to_string(),
                                    _ => "NULL".to_string(),
                                }
                            ));
                        }

                        let set_part: String = updates
                            .iter()
                            .enumerate()
                            .map(|(i, (col, _expr))| {
                                // For now, use VALUES(col) syntax or just the value
                                let val = mapped_values.get(i).cloned().unwrap_or(Value::Null);
                                match val {
                                    Value::Text(s) => {
                                        format!("{} = '{}'", col, s.replace('\'', "''"))
                                    }
                                    Value::Int(n) => format!("{} = {}", col, n),
                                    _ => format!("{} = NULL", col),
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(", ");

                        let update_query = format!(
                            "UPDATE {} SET {} {}",
                            table_name, set_part, update_condition
                        );
                        let update_session =
                            Session::new(Some(session.username.clone()), session.database.clone());

                        match self.execute(&update_query, vec![], update_session).await {
                            Ok(result) => {
                                rows_affected += result.rows_affected;
                            }
                            Err(_) => {}
                        }
                    }
                }
            }
        }

        // Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::Insert {
                tx_id: tx_id.map(|s| s.to_string()),
                table: table_name.clone(),
                values: mapped_values.clone(),
            })?;
        }

        self.mutate_state(tx_id, |state| {
            let db_state_copy = state.clone();
            let table = state
                .get_table_mut(&table_name)
                .ok_or_else(|| ExecError::TableNotFound(table_name.clone()))?;

            table.insert(self as &dyn Evaluator, mapped_values, &db_state_copy)?;

            self.refresh_materialized_views(state)?;
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected,
            transaction_id: tx_id.map(|s| s.to_string()),
            session: None,
        })
    }
}
