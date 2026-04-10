use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::InsertStmt> for Insert {
    fn from(s: ast::InsertStmt) -> Self {
        Insert {
            table: s.table,
            columns: s.columns,
            values: s.values.into_iter().map(|v| v.into()).collect(),
        }
    }
}
