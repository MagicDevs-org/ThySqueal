use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::ExecuteStmt> for Execute {
    fn from(s: ast::ExecuteStmt) -> Self {
        Execute {
            name: s.name,
            params: s.params.into_iter().map(|p| p.into()).collect(),
        }
    }
}
