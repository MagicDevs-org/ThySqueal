use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::PrepareStmt> for Prepare {
    fn from(s: ast::PrepareStmt) -> Self {
        Prepare {
            name: s.name,
            sql: s.sql,
        }
    }
}
