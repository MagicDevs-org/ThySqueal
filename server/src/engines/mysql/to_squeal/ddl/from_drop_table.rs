use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::DropTableStmt> for DropTable {
    fn from(s: ast::DropTableStmt) -> Self {
        DropTable {
            name: s.name,
            if_exists: s.if_exists,
        }
    }
}
