use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::LimitClause> for LimitClause {
    fn from(l: ast::LimitClause) -> Self {
        LimitClause {
            count: l.count,
            offset: l.offset,
        }
    }
}
