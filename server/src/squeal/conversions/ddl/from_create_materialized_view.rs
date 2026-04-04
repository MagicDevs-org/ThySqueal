use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::CreateMaterializedViewStmt> for CreateMaterializedView {
    fn from(s: ast::CreateMaterializedViewStmt) -> Self {
        CreateMaterializedView {
            name: s.name,
            query: s.query.into(),
        }
    }
}
