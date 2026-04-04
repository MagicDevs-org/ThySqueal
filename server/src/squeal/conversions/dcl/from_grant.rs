use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::GrantStmt> for Grant {
    fn from(s: ast::GrantStmt) -> Self {
        Grant {
            privileges: s.privileges,
            table: s.table,
            username: s.username,
        }
    }
}
