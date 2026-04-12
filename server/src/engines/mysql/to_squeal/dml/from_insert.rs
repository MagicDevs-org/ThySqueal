use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::InsertStmt> for Insert {
    fn from(s: ast::InsertStmt) -> Self {
        let mode = if s.replace {
            InsertMode::Replace
        } else if s.ignore {
            InsertMode::Ignore
        } else {
            InsertMode::Normal
        };
        let on_duplicate_update = s.on_duplicate_update.map(|updates| {
            updates
                .into_iter()
                .map(|(col, expr)| (col, expr.into()))
                .collect()
        });
        Insert {
            table: s.table,
            columns: s.columns,
            values: s.values.into_iter().map(|v| v.into()).collect(),
            mode,
            on_duplicate_update,
        }
    }
}
