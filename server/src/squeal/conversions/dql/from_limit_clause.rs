use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::LimitClause> for LimitClause {
    fn from(l: ast::LimitClause) -> Self {
        LimitClause {
            count: l.count,
            offset: l.offset,
        }
    }
}
