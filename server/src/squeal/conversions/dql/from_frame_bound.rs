use crate::sql::ast;
use crate::squeal::expr::*;

impl From<ast::FrameBound> for FrameBound {
    fn from(b: ast::FrameBound) -> Self {
        match b {
            ast::FrameBound::UnboundedPreceding => FrameBound::UnboundedPreceding,
            ast::FrameBound::UnboundedFollowing => FrameBound::UnboundedFollowing,
            ast::FrameBound::CurrentRow => FrameBound::CurrentRow,
            ast::FrameBound::Preceding(e) => FrameBound::Preceding(Box::new((*e).into())),
            ast::FrameBound::Following(e) => FrameBound::Following(Box::new((*e).into())),
        }
    }
}
