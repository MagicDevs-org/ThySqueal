use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::AlterTableStmt> for AlterTable {
    fn from(s: ast::AlterTableStmt) -> Self {
        AlterTable {
            table: s.table,
            action: match s.action {
                ast::AlterAction::AddColumn(c) => AlterAction::AddColumn(c),
                ast::AlterAction::DropColumn(c) => AlterAction::DropColumn(c),
                ast::AlterAction::RenameColumn { old_name, new_name } => {
                    AlterAction::RenameColumn { old_name, new_name }
                }
                ast::AlterAction::RenameTable(t) => AlterAction::RenameTable(t),
                ast::AlterAction::ModifyColumn { name, data_type } => {
                    AlterAction::ModifyColumn { name, data_type }
                }
                ast::AlterAction::SetDefault { column, value } => {
                    AlterAction::SetDefault { column, value }
                }
                ast::AlterAction::DropDefault { column } => AlterAction::DropDefault { column },
                ast::AlterAction::SetNotNull { column } => AlterAction::SetNotNull { column },
                ast::AlterAction::DropNotNull { column } => AlterAction::DropNotNull { column },
            },
        }
    }
}
