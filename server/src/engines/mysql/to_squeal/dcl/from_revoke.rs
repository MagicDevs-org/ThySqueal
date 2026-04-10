use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::RevokeStmt> for Revoke {
    fn from(s: ast::RevokeStmt) -> Self {
        Revoke {
            privileges: s.privileges,
            table: s.table,
            username: s.username,
        }
    }
}
