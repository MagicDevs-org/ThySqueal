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

impl From<ast::CreateTriggerStmt> for CreateTrigger {
    fn from(s: ast::CreateTriggerStmt) -> Self {
        CreateTrigger {
            name: s.name,
            timing: match s.timing {
                ast::TriggerTiming::Before => TriggerTiming::Before,
                ast::TriggerTiming::After => TriggerTiming::After,
            },
            event: match s.event {
                ast::TriggerEvent::Insert => TriggerEvent::Insert,
                ast::TriggerEvent::Update => TriggerEvent::Update,
                ast::TriggerEvent::Delete => TriggerEvent::Delete,
            },
            table: s.table,
            body: s.body,
        }
    }
}

impl From<ast::DropTriggerStmt> for DropTrigger {
    fn from(s: ast::DropTriggerStmt) -> Self {
        DropTrigger { name: s.name }
    }
}
