use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::UpdateStmt> for Update {
    fn from(s: ast::UpdateStmt) -> Self {
        Update {
            table: s.table,
            assignments: s
                .assignments
                .into_iter()
                .map(|(c, e)| (c, e.into()))
                .collect(),
            where_clause: s.where_clause.map(|w| w.into()),
        }
    }
}
