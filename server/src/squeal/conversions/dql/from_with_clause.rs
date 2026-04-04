use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::WithClause> for WithClause {
    fn from(w: ast::WithClause) -> Self {
        WithClause {
            recursive: w.recursive,
            ctes: w.ctes.into_iter().map(|c| c.into()).collect(),
        }
    }
}
