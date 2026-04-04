use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::PrepareStmt> for Prepare {
    fn from(s: ast::PrepareStmt) -> Self {
        Prepare {
            name: s.name,
            sql: s.sql,
        }
    }
}
