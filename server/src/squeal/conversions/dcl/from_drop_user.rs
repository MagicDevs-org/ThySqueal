use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::DropUserStmt> for DropUser {
    fn from(s: ast::DropUserStmt) -> Self {
        DropUser {
            username: s.username,
        }
    }
}
