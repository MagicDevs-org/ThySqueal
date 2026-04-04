use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::Join> for Join {
    fn from(j: ast::Join) -> Self {
        Join {
            table: j.table,
            table_alias: j.table_alias,
            join_type: match j.join_type {
                ast::JoinType::Inner => JoinType::Inner,
                ast::JoinType::Left => JoinType::Left,
            },
            on: j.on.into(),
        }
    }
}
