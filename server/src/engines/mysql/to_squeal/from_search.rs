use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::SearchStmt> for Search {
    fn from(s: ast::SearchStmt) -> Self {
        Search {
            table: s.table,
            query: s.query,
        }
    }
}
