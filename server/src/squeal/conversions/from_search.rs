use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::SearchStmt> for Search {
    fn from(s: ast::SearchStmt) -> Self {
        Search {
            table: s.table,
            query: s.query,
        }
    }
}
