use crate::engines::mysql::ast;
use crate::squeal::ir::stmt;

impl From<ast::Order> for stmt::Order {
    fn from(o: ast::Order) -> Self {
        match o {
            ast::Order::Asc => stmt::Order::Asc,
            ast::Order::Desc => stmt::Order::Desc,
        }
    }
}

impl From<ast::OrderByItem> for stmt::OrderByItem {
    fn from(o: ast::OrderByItem) -> Self {
        stmt::OrderByItem {
            expr: o.expr.into(),
            order: o.order.into(),
        }
    }
}

impl From<ast::WindowOrderByItem> for stmt::OrderByItem {
    fn from(o: ast::WindowOrderByItem) -> Self {
        stmt::OrderByItem {
            expr: o.expr.into(),
            order: o.order.into(),
        }
    }
}
