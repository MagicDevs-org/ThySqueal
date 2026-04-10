use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::DropUserStmt> for DropUser {
    fn from(s: ast::DropUserStmt) -> Self {
        DropUser {
            username: s.username,
        }
    }
}
