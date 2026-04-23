use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::Join> for Join {
    fn from(j: ast::Join) -> Self {
        Join {
            table: j.table,
            table_alias: j.table_alias,
            join_type: match j.join_type {
                ast::JoinType::Inner => JoinType::Inner,
                ast::JoinType::Left => JoinType::Left,
                ast::JoinType::Right => JoinType::Right,
            },
            on: j.on.into(),
        }
    }
}
