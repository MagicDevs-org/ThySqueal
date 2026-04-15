use super::super::{Executor, QueryResult};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::{CreateTable, Expression};
use crate::storage::{Table, WalRecord};

impl Executor {
    pub async fn exec_create_table(
        &self,
        stmt: CreateTable,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        let name = stmt.name.clone();
        let columns = stmt.columns.clone();
        let primary_key = stmt.primary_key.clone();
        let foreign_keys = stmt.foreign_keys.clone();

        // 1. Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateTable {
                tx_id: tx_id.map(|s| s.to_string()),
                name: name.clone(),
                columns: columns.clone(),
                primary_key: primary_key.clone(),
                foreign_keys: foreign_keys.clone(),
            })?;
        }

        // 2. Mutate state
        self.mutate_state(tx_id, |state| {
            if state.tables.contains_key(&name) {
                return Err(ExecError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Table {} already exists",
                        name
                    )),
                ));
            }

            let mut table = Table::new(
                name.clone(),
                columns,
                primary_key.clone(),
                foreign_keys.clone(),
            );

            // Enable search index automatically
            if let Some(ref dir) = self.data_dir {
                table.enable_search_index(dir);
            }

            // If primary key is defined, create a unique B-Tree index for it
            if let Some(ref pk_cols) = primary_key {
                let pk_exprs: Vec<Expression> = pk_cols
                    .iter()
                    .map(|c| Expression::Column(c.clone()))
                    .collect();

                table.create_index(
                    self,
                    format!("pk_{}", name),
                    pk_exprs,
                    true,  // unique
                    false, // btree
                    None,  // no where clause
                    state,
                )?;
            }

            state.tables.insert(name, table);
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
            session: None,
        })
    }
}
