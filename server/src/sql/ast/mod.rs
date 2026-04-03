pub mod condition;
pub mod expression;
pub mod statements;

pub use condition::*;
pub use expression::*;
pub use statements::*;

use crate::squeal;
use serde::{Deserialize, Serialize};

/// Parsed SQL statement AST.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlStmt {
    CreateTable(CreateTableStmt),
    CreateMaterializedView(CreateMaterializedViewStmt),
    AlterTable(AlterTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
    CreateUser(CreateUserStmt),
    DropUser(DropUserStmt),
    Grant(GrantStmt),
    Revoke(RevokeStmt),
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    Explain(SelectStmt),
    Search(SearchStmt),
    Prepare(PrepareStmt),
    Execute(ExecuteStmt),
    Deallocate(String),
    Set(SetStmt),
    Begin,
    Commit,
    Rollback,
}

impl SqlStmt {
    pub fn resolve_placeholders(&mut self) {
        let mut counter = 0;
        match self {
            SqlStmt::Select(s) => s.resolve_placeholders(&mut counter),
            SqlStmt::Update(u) => u.resolve_placeholders(&mut counter),
            SqlStmt::Delete(d) => d.resolve_placeholders(&mut counter),
            SqlStmt::Explain(s) => s.resolve_placeholders(&mut counter),
            SqlStmt::CreateIndex(ci) => ci.resolve_placeholders(&mut counter),
            SqlStmt::CreateMaterializedView(mv) => mv.query.resolve_placeholders(&mut counter),
            SqlStmt::Insert(i) => i.resolve_placeholders(&mut counter),
            SqlStmt::Set(s) => s.resolve_placeholders(&mut counter),
            // No placeholders in these statements
            SqlStmt::CreateTable(_)
            | SqlStmt::AlterTable(_)
            | SqlStmt::DropTable(_)
            | SqlStmt::CreateUser(_)
            | SqlStmt::DropUser(_)
            | SqlStmt::Grant(_)
            | SqlStmt::Revoke(_)
            | SqlStmt::Search(_)
            | SqlStmt::Begin
            | SqlStmt::Commit
            | SqlStmt::Rollback
            | SqlStmt::Prepare(_)
            | SqlStmt::Execute(_)
            | SqlStmt::Deallocate(_) => {}
        }
    }
}

// Conversions from Squeal IR to AST
impl From<squeal::Select> for SelectStmt {
    fn from(s: squeal::Select) -> Self {
        SelectStmt {
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
        }
    }
}

impl From<squeal::WithClause> for WithClause {
    fn from(w: squeal::WithClause) -> Self {
        WithClause {
            ctes: w.ctes.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<squeal::Cte> for Cte {
    fn from(c: squeal::Cte) -> Self {
        Cte {
            name: c.name,
            query: c.query.into(),
        }
    }
}

impl From<squeal::SelectColumn> for SelectColumn {
    fn from(c: squeal::SelectColumn) -> Self {
        SelectColumn {
            expr: c.expr.into(),
            alias: c.alias,
        }
    }
}

impl From<squeal::Join> for Join {
    fn from(j: squeal::Join) -> Self {
        Join {
            table: j.table,
            table_alias: j.table_alias,
            join_type: match j.join_type {
                squeal::JoinType::Inner => JoinType::Inner,
                squeal::JoinType::Left => JoinType::Left,
            },
            on: j.on.into(),
        }
    }
}

impl From<squeal::stmt::OrderByItem> for OrderByItem {
    fn from(o: squeal::stmt::OrderByItem) -> Self {
        OrderByItem {
            expr: o.expr.into(),
            order: match o.order {
                squeal::stmt::Order::Asc => Order::Asc,
                squeal::stmt::Order::Desc => Order::Desc,
            },
        }
    }
}

impl From<squeal::expr::OrderByItem> for WindowOrderByItem {
    fn from(o: squeal::expr::OrderByItem) -> Self {
        WindowOrderByItem {
            expr: o.expr.into(),
            ascending: o.ascending,
        }
    }
}

impl From<squeal::LimitClause> for LimitClause {
    fn from(l: squeal::LimitClause) -> Self {
        LimitClause {
            count: l.count,
            offset: l.offset,
        }
    }
}

impl From<squeal::Expression> for Expression {
    fn from(e: squeal::Expression) -> Self {
        match e {
            squeal::Expression::Literal(v) => Expression::Literal(v),
            squeal::Expression::Placeholder(i) => Expression::Placeholder(i),
            squeal::Expression::Column(c) => Expression::Column(c),
            squeal::Expression::BinaryOp(l, op, r) => Expression::BinaryOp(
                Box::new((*l).into()),
                match op {
                    squeal::BinaryOp::Add => BinaryOp::Add,
                    squeal::BinaryOp::Sub => BinaryOp::Sub,
                    squeal::BinaryOp::Mul => BinaryOp::Mul,
                    squeal::BinaryOp::Div => BinaryOp::Div,
                },
                Box::new((*r).into()),
            ),
            squeal::Expression::FunctionCall(f) => Expression::FunctionCall(FunctionCall {
                name: match f.name {
                    squeal::AggregateType::Count => AggregateType::Count,
                    squeal::AggregateType::Sum => AggregateType::Sum,
                    squeal::AggregateType::Avg => AggregateType::Avg,
                    squeal::AggregateType::Min => AggregateType::Min,
                    squeal::AggregateType::Max => AggregateType::Max,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
            }),
            squeal::Expression::ScalarFunc(f) => Expression::ScalarFunc(ScalarFunction {
                name: match f.name {
                    squeal::ScalarFuncType::Lower => ScalarFuncType::Lower,
                    squeal::ScalarFuncType::Upper => ScalarFuncType::Upper,
                    squeal::ScalarFuncType::Length => ScalarFuncType::Length,
                    squeal::ScalarFuncType::Abs => ScalarFuncType::Abs,
                    squeal::ScalarFuncType::Now => ScalarFuncType::Now,
                    squeal::ScalarFuncType::Concat => ScalarFuncType::Concat,
                    squeal::ScalarFuncType::Coalesce => ScalarFuncType::Coalesce,
                    squeal::ScalarFuncType::Replace => ScalarFuncType::Replace,
                    squeal::ScalarFuncType::IfNull => ScalarFuncType::IfNull,
                    squeal::ScalarFuncType::If => ScalarFuncType::If,
                    squeal::ScalarFuncType::DateDiff => ScalarFuncType::DateDiff,
                    squeal::ScalarFuncType::DateFormat => ScalarFuncType::DateFormat,
                    squeal::ScalarFuncType::Md5 => ScalarFuncType::Md5,
                    squeal::ScalarFuncType::Sha2 => ScalarFuncType::Sha2,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
            }),
            squeal::Expression::Star => Expression::Star,
            squeal::Expression::Variable(v) => Expression::Variable(Variable {
                name: v.name,
                is_system: v.is_system,
                scope: match v.scope {
                    squeal::VariableScope::Global => VariableScope::Global,
                    squeal::VariableScope::Session => VariableScope::Session,
                    squeal::VariableScope::User => VariableScope::User,
                },
            }),
            squeal::Expression::Subquery(s) => Expression::Subquery(Box::new((*s).into())),
            squeal::Expression::UnaryNot(e) => Expression::UnaryNot(Box::new((*e).into())),
            squeal::Expression::WindowFunc(f) => Expression::WindowFunc(WindowFunction {
                func_type: match f.func_type {
                    squeal::WindowFuncType::RowNumber => WindowFuncType::RowNumber,
                    squeal::WindowFuncType::Rank => WindowFuncType::Rank,
                    squeal::WindowFuncType::DenseRank => WindowFuncType::DenseRank,
                    squeal::WindowFuncType::Ntile => WindowFuncType::Ntile,
                    squeal::WindowFuncType::PercentRank => WindowFuncType::PercentRank,
                    squeal::WindowFuncType::CumeDist => WindowFuncType::CumeDist,
                    squeal::WindowFuncType::FirstValue => WindowFuncType::FirstValue,
                    squeal::WindowFuncType::LastValue => WindowFuncType::LastValue,
                    squeal::WindowFuncType::NthValue => WindowFuncType::NthValue,
                    squeal::WindowFuncType::Lag => WindowFuncType::Lag,
                    squeal::WindowFuncType::Lead => WindowFuncType::Lead,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
                partition_by: f.partition_by.into_iter().map(|e| e.into()).collect(),
                order_by: f.order_by.into_iter().map(|o| o.into()).collect(),
                frame: f.frame.map(|f| Box::new((*f).into())),
            }),
        }
    }
}

impl From<squeal::Condition> for Condition {
    fn from(c: squeal::Condition) -> Self {
        match c {
            squeal::Condition::And(l, r) => {
                Condition::And(Box::new((*l).into()), Box::new((*r).into()))
            }
            squeal::Condition::Or(l, r) => {
                Condition::Or(Box::new((*l).into()), Box::new((*r).into()))
            }
            squeal::Condition::Not(c) => Condition::Not(Box::new((*c).into())),
            squeal::Condition::Comparison(l, op, r) => Condition::Comparison(
                l.into(),
                match op {
                    squeal::ComparisonOp::Eq => ComparisonOp::Eq,
                    squeal::ComparisonOp::Neq => ComparisonOp::Neq,
                    squeal::ComparisonOp::Gt => ComparisonOp::Gt,
                    squeal::ComparisonOp::Gte => ComparisonOp::Gte,
                    squeal::ComparisonOp::Lt => ComparisonOp::Lt,
                    squeal::ComparisonOp::Lte => ComparisonOp::Lte,
                },
                r.into(),
            ),
            squeal::Condition::In(e, v) => Condition::In(
                e.into(),
                v.into_iter()
                    .map(|x: squeal::Expression| x.into())
                    .collect(),
            ),
            squeal::Condition::InSubquery(e, s) => {
                Condition::InSubquery(e.into(), Box::new((*s).into()))
            }
            squeal::Condition::Exists(s) => Condition::Exists(Box::new((*s).into())),
            squeal::Condition::Between(e, l, h) => Condition::Between(e.into(), l.into(), h.into()),
            squeal::Condition::Is(e, op) => Condition::Is(
                e.into(),
                match op {
                    squeal::IsOp::Null => IsOp::Null,
                    squeal::IsOp::NotNull => IsOp::NotNull,
                    squeal::IsOp::True => IsOp::True,
                    squeal::IsOp::False => IsOp::False,
                },
            ),
            squeal::Condition::Like(e, s) => Condition::Like(e.into(), s),
            squeal::Condition::FullTextSearch(f, q) => Condition::FullTextSearch(f, q),
        }
    }
}

impl From<squeal::WindowFrame> for WindowFrame {
    fn from(f: squeal::WindowFrame) -> Self {
        WindowFrame {
            units: match f.units {
                squeal::FrameUnits::Rows => FrameUnits::Rows,
                squeal::FrameUnits::Range => FrameUnits::Range,
            },
            start: Box::new((*f.start).into()),
            end: Box::new((*f.end).into()),
        }
    }
}

impl From<squeal::FrameBound> for FrameBound {
    fn from(b: squeal::FrameBound) -> Self {
        match b {
            squeal::FrameBound::UnboundedPreceding => FrameBound::UnboundedPreceding,
            squeal::FrameBound::UnboundedFollowing => FrameBound::UnboundedFollowing,
            squeal::FrameBound::CurrentRow => FrameBound::CurrentRow,
            squeal::FrameBound::Preceding(e) => FrameBound::Preceding(Box::new((*e).into())),
            squeal::FrameBound::Following(e) => FrameBound::Following(Box::new((*e).into())),
        }
    }
}
