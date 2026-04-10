use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::CreateMaterializedViewStmt> for CreateMaterializedView {
    fn from(s: ast::CreateMaterializedViewStmt) -> Self {
        CreateMaterializedView {
            name: s.name,
            query: s.query.into(),
        }
    }
}
