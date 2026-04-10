use super::expression::Expression;
use super::statements::SelectStmt;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Condition {
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
    Comparison(Expression, ComparisonOp, Expression),
    In(Expression, Vec<Expression>),
    InSubquery(Expression, Box<SelectStmt>),
    Exists(Box<SelectStmt>),
    Between(Expression, Expression, Expression),
    Is(Expression, IsOp),
    Like(Expression, String),
    FullTextSearch(String, String), // field, query
    // Restoring for parser compatibility
    Logical(Box<Condition>, LogicalOp, Box<Condition>),
    IsNull(Expression),
    IsNotNull(Expression),
}

impl Condition {
    pub fn resolve_placeholders(&mut self, counter: &mut usize) {
        match self {
            Condition::And(l, r) => {
                l.resolve_placeholders(counter);
                r.resolve_placeholders(counter);
            }
            Condition::Or(l, r) => {
                l.resolve_placeholders(counter);
                r.resolve_placeholders(counter);
            }
            Condition::Not(c) => {
                c.resolve_placeholders(counter);
            }
            Condition::Comparison(l, _, r) => {
                l.resolve_placeholders(counter);
                r.resolve_placeholders(counter);
            }
            Condition::In(e, v) => {
                e.resolve_placeholders(counter);
                for ve in v {
                    ve.resolve_placeholders(counter);
                }
            }
            Condition::InSubquery(e, s) => {
                e.resolve_placeholders(counter);
                s.resolve_placeholders(counter);
            }
            Condition::Exists(s) => {
                s.resolve_placeholders(counter);
            }
            Condition::Between(e, l, h) => {
                e.resolve_placeholders(counter);
                l.resolve_placeholders(counter);
                h.resolve_placeholders(counter);
            }
            Condition::Is(e, _) => {
                e.resolve_placeholders(counter);
            }
            Condition::Like(e, _) => {
                e.resolve_placeholders(counter);
            }
            Condition::FullTextSearch(_, _) => {}
            Condition::Logical(l, _, r) => {
                l.resolve_placeholders(counter);
                r.resolve_placeholders(counter);
            }
            Condition::IsNull(e) => {
                e.resolve_placeholders(counter);
            }
            Condition::IsNotNull(e) => {
                e.resolve_placeholders(counter);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    // Restoring for parser compatibility
    NotEq,
    LtEq,
    GtEq,
    Like,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsOp {
    Null,
    NotNull,
    True,
    False,
}
