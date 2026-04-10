use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::CreateUserStmt> for CreateUser {
    fn from(s: ast::CreateUserStmt) -> Self {
        CreateUser {
            username: s.username,
            password: s.password,
        }
    }
}
