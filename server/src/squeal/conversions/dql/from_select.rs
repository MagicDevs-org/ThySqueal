use crate::sql::ast;
use crate::squeal::stmt::*;

impl From<ast::SelectStmt> for Select {
    fn from(s: ast::SelectStmt) -> Self {
        Select {
            with_clause: s.with_clause.map(|w| w.into()),
            columns: s.columns.into_iter().map(|c| c.into()).collect(),
            table: s.table,
            table_alias: s.table_alias,
            distinct: s.distinct,
            joins: s.joins.into_iter().map(|j| j.into()).collect(),
            where_clause: s.where_clause.map(|w| w.into()),
            group_by: s.group_by.into_iter().map(|g| g.into()).collect(),
            having: s.having.map(|h| h.into()),
            order_by: s.order_by.into_iter().map(|o| o.into()).collect(),
            limit: s.limit.map(|l| l.into()),
            set_operations: s.set_operations.into_iter().map(|s| s.into()).collect(),
        }
    }
}
