use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::CreateDatabaseStmt> for CreateDatabase {
    fn from(s: ast::CreateDatabaseStmt) -> Self {
        CreateDatabase {
            name: s.name,
            if_not_exists: s.if_not_exists,
        }
    }
}

impl From<ast::DropDatabaseStmt> for DropDatabase {
    fn from(s: ast::DropDatabaseStmt) -> Self {
        DropDatabase {
            name: s.name,
            if_exists: s.if_exists,
        }
    }
}
