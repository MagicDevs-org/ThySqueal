use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::SelectColumn> for SelectColumn {
    fn from(c: ast::SelectColumn) -> Self {
        SelectColumn {
            expr: c.expr.into(),
            alias: c.alias,
        }
    }
}
