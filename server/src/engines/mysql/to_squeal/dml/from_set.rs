use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::SetStmt> for Set {
    fn from(s: ast::SetStmt) -> Self {
        Set {
            assignments: s
                .assignments
                .into_iter()
                .map(|(v, e)| (v.into(), e.into()))
                .collect(),
        }
    }
}
