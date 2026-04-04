use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::DropTableStmt> for DropTable {
    fn from(s: ast::DropTableStmt) -> Self {
        DropTable { name: s.name }
    }
}
