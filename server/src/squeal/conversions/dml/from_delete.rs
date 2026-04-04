use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::DeleteStmt> for Delete {
    fn from(s: ast::DeleteStmt) -> Self {
        Delete {
            table: s.table,
            where_clause: s.where_clause.map(|w| w.into()),
        }
    }
}
