use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::CreateUserStmt> for CreateUser {
    fn from(s: ast::CreateUserStmt) -> Self {
        CreateUser {
            username: s.username,
            password: s.password,
        }
    }
}
