use super::super::{Executor, QueryResult};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::{AlterAction, AlterTable};
use crate::storage::WalRecord;

impl Executor {
    pub async fn exec_alter_table(
        &self,
        stmt: AlterTable,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        // 1. Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::AlterTable {
                tx_id: tx_id.map(|s| s.to_string()),
                table: stmt.table.clone(),
                action: stmt.action.clone(),
            })?;
        }

        // 2. Mutate state
        self.mutate_state(tx_id, |state| {
            match stmt.action {
                AlterAction::AddColumn(col) => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.add_column(col)?;
                }
                AlterAction::DropColumn(col_name) => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.drop_column(&col_name)?;
                }
                AlterAction::RenameColumn { old_name, new_name } => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.rename_column(&old_name, &new_name)?;
                }
                AlterAction::RenameTable(new_name) => {
                    let table = state
                        .tables
                        .remove(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    let mut table = table;
                    table.rename_table(new_name.clone());
                    state.tables.insert(new_name, table);
                }
                AlterAction::ModifyColumn { name, data_type } => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.modify_column_type(&name, data_type.clone())?;
                }
                AlterAction::SetDefault { column, value } => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.set_column_default(&column, value.clone())?;
                }
                AlterAction::DropDefault { column } => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.set_column_default(&column, None)?;
                }
                AlterAction::SetNotNull { column } => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.set_column_not_null(&column, true)?;
                }
                AlterAction::DropNotNull { column } => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.set_column_not_null(&column, false)?;
                }
                AlterAction::AddPrimaryKey { columns } => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.set_primary_key(columns)?;
                }
                AlterAction::DropPrimaryKey => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.set_primary_key(vec![])?;
                }
                AlterAction::AddForeignKey {
                    name,
                    columns,
                    ref_table,
                    ref_columns,
                } => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.add_foreign_key(crate::storage::ForeignKey {
                        name: name.unwrap_or_default(),
                        columns,
                        ref_table,
                        ref_columns,
                    })?;
                }
                AlterAction::DropForeignKey { name } => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
                    table.drop_foreign_key(&name)?;
                }
                AlterAction::AlterEngine { engine: _ } => {
                    // Engine is metadata, not stored in table schema currently
                }
                AlterAction::AlterCharset {
                    charset: _,
                    collation: _,
                } => {
                    // Charset/collation is metadata, not stored in table schema currently
                }
            }
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
