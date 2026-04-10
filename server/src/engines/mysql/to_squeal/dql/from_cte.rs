use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::Cte> for Cte {
    fn from(c: ast::Cte) -> Self {
        Cte {
            name: c.name,
            query: c.query.into(),
        }
    }
}
