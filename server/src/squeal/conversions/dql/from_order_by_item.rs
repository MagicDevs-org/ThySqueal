use crate::sql::ast;
use crate::squeal::expr;
use crate::squeal::stmt;

impl From<ast::OrderByItem> for stmt::OrderByItem {
    fn from(o: ast::OrderByItem) -> Self {
        stmt::OrderByItem {
            expr: o.expr.into(),
            order: match o.order {
                ast::Order::Asc => stmt::Order::Asc,
                ast::Order::Desc => stmt::Order::Desc,
            },
        }
    }
}

impl From<ast::WindowOrderByItem> for expr::OrderByItem {
    fn from(o: ast::WindowOrderByItem) -> Self {
        expr::OrderByItem {
            expr: o.expr.into(),
            ascending: o.ascending,
        }
    }
}
