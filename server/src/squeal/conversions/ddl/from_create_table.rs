use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::CreateTableStmt> for CreateTable {
    fn from(s: ast::CreateTableStmt) -> Self {
        CreateTable {
            name: s.name,
            columns: s.columns,
            primary_key: s.primary_key,
            foreign_keys: s.foreign_keys,
        }
    }
}
