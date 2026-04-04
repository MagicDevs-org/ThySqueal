use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::SelectColumn> for SelectColumn {
    fn from(c: ast::SelectColumn) -> Self {
        SelectColumn {
            expr: c.expr.into(),
            alias: c.alias,
        }
    }
}
