use crate::sql::ast;
use crate::squeal::cond::*;

impl From<ast::Condition> for Condition {
    fn from(c: ast::Condition) -> Self {
        match c {
            ast::Condition::And(l, r) => {
                Condition::And(Box::new((*l).into()), Box::new((*r).into()))
            }
            ast::Condition::Or(l, r) => Condition::Or(Box::new((*l).into()), Box::new((*r).into())),
            ast::Condition::Not(c) => Condition::Not(Box::new((*c).into())),
            ast::Condition::Comparison(l, op, r) => Condition::Comparison(
                l.into(),
                match op {
                    ast::ComparisonOp::Eq => ComparisonOp::Eq,
                    ast::ComparisonOp::Neq | ast::ComparisonOp::NotEq => ComparisonOp::Neq,
                    ast::ComparisonOp::Gt => ComparisonOp::Gt,
                    ast::ComparisonOp::Gte | ast::ComparisonOp::GtEq => ComparisonOp::Gte,
                    ast::ComparisonOp::Lt => ComparisonOp::Lt,
                    ast::ComparisonOp::Lte | ast::ComparisonOp::LtEq => ComparisonOp::Lte,
                    ast::ComparisonOp::Like => ComparisonOp::Eq, // LIKE handled specially
                },
                r.into(),
            ),
            ast::Condition::In(e, v) => Condition::In(
                e.into(),
                v.into_iter().map(|x: ast::Expression| x.into()).collect(),
            ),
            ast::Condition::InSubquery(e, s) => {
                Condition::InSubquery(e.into(), Box::new((*s).into()))
            }
            ast::Condition::Exists(s) => Condition::Exists(Box::new((*s).into())),
            ast::Condition::Between(e, l, h) => Condition::Between(e.into(), l.into(), h.into()),
            ast::Condition::Is(e, op) => Condition::Is(
                e.into(),
                match op {
                    ast::IsOp::Null => IsOp::Null,
                    ast::IsOp::NotNull => IsOp::NotNull,
                    ast::IsOp::True => IsOp::True,
                    ast::IsOp::False => IsOp::False,
                },
            ),
            ast::Condition::Like(e, s) => Condition::Like(e.into(), s),
            ast::Condition::FullTextSearch(f, q) => Condition::FullTextSearch(f, q),
            ast::Condition::Logical(l, op, r) => match op {
                ast::LogicalOp::And => Condition::And(Box::new((*l).into()), Box::new((*r).into())),
                ast::LogicalOp::Or => Condition::Or(Box::new((*l).into()), Box::new((*r).into())),
            },
            ast::Condition::IsNull(e) => Condition::Is(e.into(), IsOp::Null),
            ast::Condition::IsNotNull(e) => Condition::Is(e.into(), IsOp::NotNull),
        }
    }
}
