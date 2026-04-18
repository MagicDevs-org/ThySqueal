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
    CreateDatabase(CreateDatabaseStmt),
    DropDatabase(DropDatabaseStmt),
    CreateTrigger(CreateTriggerStmt),
    DropTrigger(DropTriggerStmt),
    CreateMaterializedView(CreateMaterializedViewStmt),
    CreateView(CreateViewStmt),
    AlterView(AlterViewStmt),
    DropView(DropViewStmt),
    CreateProcedure(CreateProcedureStmt),
    DropProcedure(DropProcedureStmt),
    CreateFunction(CreateFunctionStmt),
    DropFunction(DropFunctionStmt),
    Call(CallStmt),
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
    Describe(String),
    Use(String),
    Search(SearchStmt),
    Prepare(PrepareStmt),
    Execute(ExecuteStmt),
    Deallocate(String),
    Set(SetStmt),
    Kill(KillStmt),
    Show(ShowStmt),
    Begin,
    BeginEndBlock(Vec<VariableDeclaration>, Vec<SqlStmt>),
    If(IfStmt),
    Case(CaseStmt),
    While(WhileStmt),
    Repeat(RepeatStmt),
    Loop(LoopStmt),
    Commit,
    Rollback,
    Savepoint(squeal::ir::stmt::SavepointStmt),
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
            SqlStmt::CreateView(cv) => cv.query.resolve_placeholders(&mut counter),
            SqlStmt::AlterView(av) => av.query.resolve_placeholders(&mut counter),
            SqlStmt::Insert(i) => i.resolve_placeholders(&mut counter),
            SqlStmt::Set(s) => s.resolve_placeholders(&mut counter),
            // No placeholders in these statements
            SqlStmt::CreateTable(_)
            | SqlStmt::CreateDatabase(_)
            | SqlStmt::DropDatabase(_)
            | SqlStmt::CreateTrigger(_)
            | SqlStmt::DropTrigger(_)
            | SqlStmt::DropView(_)
            | SqlStmt::Begin
            | SqlStmt::Commit
            | SqlStmt::Rollback
            | SqlStmt::Kill(_)
            | SqlStmt::Show(_)
            | SqlStmt::Savepoint(_)
            | SqlStmt::Prepare(_)
            | SqlStmt::Execute(_)
            | SqlStmt::Deallocate(_) => {}
            _ => {}
        }
    }
}

// Conversions from Squeal IR to AST
impl From<squeal::ir::Select> for SelectStmt {
    fn from(s: squeal::ir::Select) -> Self {
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
            set_operations: s.set_operations.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl From<squeal::ir::WithClause> for WithClause {
    fn from(w: squeal::ir::WithClause) -> Self {
        WithClause {
            recursive: w.recursive,
            ctes: w.ctes.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<squeal::ir::Cte> for Cte {
    fn from(c: squeal::ir::Cte) -> Self {
        Cte {
            name: c.name,
            query: c.query.into(),
        }
    }
}

impl From<squeal::ir::SelectColumn> for SelectColumn {
    fn from(c: squeal::ir::SelectColumn) -> Self {
        SelectColumn {
            expr: c.expr.into(),
            alias: c.alias,
        }
    }
}

impl From<squeal::ir::Join> for Join {
    fn from(j: squeal::ir::Join) -> Self {
        Join {
            table: j.table,
            table_alias: j.table_alias,
            join_type: match j.join_type {
                squeal::ir::JoinType::Inner => JoinType::Inner,
                squeal::ir::JoinType::Left => JoinType::Left,
            },
            on: j.on.into(),
        }
    }
}

impl From<squeal::ir::Order> for Order {
    fn from(o: squeal::ir::Order) -> Self {
        match o {
            squeal::ir::Order::Asc => Order::Asc,
            squeal::ir::Order::Desc => Order::Desc,
        }
    }
}

impl From<squeal::ir::OrderByItem> for OrderByItem {
    fn from(o: squeal::ir::OrderByItem) -> Self {
        OrderByItem {
            expr: o.expr.into(),
            order: o.order.into(),
        }
    }
}

impl From<squeal::ir::OrderByItem> for WindowOrderByItem {
    fn from(o: squeal::ir::OrderByItem) -> Self {
        WindowOrderByItem {
            expr: o.expr.into(),
            order: o.order.into(),
        }
    }
}

impl From<squeal::ir::LimitClause> for LimitClause {
    fn from(l: squeal::ir::LimitClause) -> Self {
        LimitClause {
            count: l.count,
            offset: l.offset,
        }
    }
}

impl From<squeal::ir::Expression> for Expression {
    fn from(e: squeal::ir::Expression) -> Self {
        match e {
            squeal::ir::Expression::Literal(v) => Expression::Literal(v),
            squeal::ir::Expression::Placeholder(i) => Expression::Placeholder(i),
            squeal::ir::Expression::Column(c) => Expression::Column(c),
            squeal::ir::Expression::BinaryOp(l, op, r) => Expression::BinaryOp(
                Box::new((*l).into()),
                match op {
                    squeal::ir::BinaryOp::Add => BinaryOp::Add,
                    squeal::ir::BinaryOp::Sub => BinaryOp::Sub,
                    squeal::ir::BinaryOp::Mul => BinaryOp::Mul,
                    squeal::ir::BinaryOp::Div => BinaryOp::Div,
                },
                Box::new((*r).into()),
            ),
            squeal::ir::Expression::FunctionCall(f) => Expression::FunctionCall(FunctionCall {
                name: match f.name {
                    squeal::ir::AggregateType::Count => AggregateType::Count,
                    squeal::ir::AggregateType::Sum => AggregateType::Sum,
                    squeal::ir::AggregateType::Avg => AggregateType::Avg,
                    squeal::ir::AggregateType::Min => AggregateType::Min,
                    squeal::ir::AggregateType::Max => AggregateType::Max,
                    squeal::ir::AggregateType::GroupConcat => AggregateType::GroupConcat,
                    squeal::ir::AggregateType::JsonArrayAgg => AggregateType::JsonArrayAgg,
                    squeal::ir::AggregateType::JsonObjectAgg => AggregateType::JsonObjectAgg,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
            }),
            squeal::ir::Expression::ScalarFunc(f) => Expression::ScalarFunc(ScalarFunction {
                name: match f.name {
                    squeal::ir::ScalarFuncType::Lower => ScalarFuncType::Lower,
                    squeal::ir::ScalarFuncType::Upper => ScalarFuncType::Upper,
                    squeal::ir::ScalarFuncType::Length => ScalarFuncType::Length,
                    squeal::ir::ScalarFuncType::Abs => ScalarFuncType::Abs,
                    squeal::ir::ScalarFuncType::Now => ScalarFuncType::Now,
                    squeal::ir::ScalarFuncType::Concat => ScalarFuncType::Concat,
                    squeal::ir::ScalarFuncType::Coalesce => ScalarFuncType::Coalesce,
                    squeal::ir::ScalarFuncType::Replace => ScalarFuncType::Replace,
                    squeal::ir::ScalarFuncType::IfNull => ScalarFuncType::IfNull,
                    squeal::ir::ScalarFuncType::If => ScalarFuncType::If,
                    squeal::ir::ScalarFuncType::DateDiff => ScalarFuncType::DateDiff,
                    squeal::ir::ScalarFuncType::DateFormat => ScalarFuncType::DateFormat,
                    squeal::ir::ScalarFuncType::Md5 => ScalarFuncType::Md5,
                    squeal::ir::ScalarFuncType::Sha2 => ScalarFuncType::Sha2,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
            }),
            squeal::ir::Expression::Star => Expression::Star,
            squeal::ir::Expression::Variable(v) => Expression::Variable(Variable {
                name: v.name,
                is_system: v.is_system,
                scope: match v.scope {
                    squeal::ir::VariableScope::Global => VariableScope::Global,
                    squeal::ir::VariableScope::Session => VariableScope::Session,
                    squeal::ir::VariableScope::User => VariableScope::User,
                },
            }),
            squeal::ir::Expression::Subquery(s) => Expression::Subquery(Box::new((*s).into())),
            squeal::ir::Expression::UnaryNot(e) => Expression::UnaryNot(Box::new((*e).into())),
            squeal::ir::Expression::WindowFunc(f) => Expression::WindowFunc(WindowFunction {
                func_type: match f.func_type {
                    squeal::ir::WindowFuncType::RowNumber => WindowFuncType::RowNumber,
                    squeal::ir::WindowFuncType::Rank => WindowFuncType::Rank,
                    squeal::ir::WindowFuncType::DenseRank => WindowFuncType::DenseRank,
                    squeal::ir::WindowFuncType::Ntile => WindowFuncType::Ntile,
                    squeal::ir::WindowFuncType::PercentRank => WindowFuncType::PercentRank,
                    squeal::ir::WindowFuncType::CumeDist => WindowFuncType::CumeDist,
                    squeal::ir::WindowFuncType::FirstValue => WindowFuncType::FirstValue,
                    squeal::ir::WindowFuncType::LastValue => WindowFuncType::LastValue,
                    squeal::ir::WindowFuncType::NthValue => WindowFuncType::NthValue,
                    squeal::ir::WindowFuncType::Lag => WindowFuncType::Lag,
                    squeal::ir::WindowFuncType::Lead => WindowFuncType::Lead,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
                partition_by: f.partition_by.into_iter().map(|e| e.into()).collect(),
                order_by: f.order_by.into_iter().map(|o| o.into()).collect(),
                frame: f.frame.map(|f| Box::new((*f).into())),
            }),
            squeal::ir::Expression::CaseWhen(cw) => Expression::CaseWhen(CaseWhen {
                conditions: cw
                    .conditions
                    .into_iter()
                    .map(|(cond, then)| (cond.into(), then.into()))
                    .collect(),
                else_expr: cw.else_expr.map(|e| Box::new((*e).into())),
            }),
        }
    }
}

impl From<squeal::ir::Condition> for Condition {
    fn from(c: squeal::ir::Condition) -> Self {
        match c {
            squeal::ir::Condition::And(l, r) => {
                Condition::And(Box::new((*l).into()), Box::new((*r).into()))
            }
            squeal::ir::Condition::Or(l, r) => {
                Condition::Or(Box::new((*l).into()), Box::new((*r).into()))
            }
            squeal::ir::Condition::Not(c) => Condition::Not(Box::new((*c).into())),
            squeal::ir::Condition::Comparison(l, op, r) => Condition::Comparison(
                l.into(),
                match op {
                    squeal::ir::ComparisonOp::Eq => ComparisonOp::Eq,
                    squeal::ir::ComparisonOp::Neq => ComparisonOp::Neq,
                    squeal::ir::ComparisonOp::Gt => ComparisonOp::Gt,
                    squeal::ir::ComparisonOp::Gte => ComparisonOp::Gte,
                    squeal::ir::ComparisonOp::Lt => ComparisonOp::Lt,
                    squeal::ir::ComparisonOp::Lte => ComparisonOp::Lte,
                },
                r.into(),
            ),
            squeal::ir::Condition::In(e, v) => Condition::In(
                e.into(),
                v.into_iter()
                    .map(|x: squeal::ir::Expression| x.into())
                    .collect(),
            ),
            squeal::ir::Condition::InSubquery(e, s) => {
                Condition::InSubquery(e.into(), Box::new((*s).into()))
            }
            squeal::ir::Condition::Exists(s) => Condition::Exists(Box::new((*s).into())),
            squeal::ir::Condition::Between(e, l, h) => {
                Condition::Between(e.into(), l.into(), h.into())
            }
            squeal::ir::Condition::Is(e, op) => Condition::Is(
                e.into(),
                match op {
                    squeal::ir::IsOp::Null => IsOp::Null,
                    squeal::ir::IsOp::NotNull => IsOp::NotNull,
                    squeal::ir::IsOp::True => IsOp::True,
                    squeal::ir::IsOp::False => IsOp::False,
                },
            ),
            squeal::ir::Condition::Like(e, s) => Condition::Like(e.into(), s),
            squeal::ir::Condition::FullTextSearch(f, q) => Condition::FullTextSearch(f, q),
        }
    }
}

impl From<squeal::ir::WindowFrame> for WindowFrame {
    fn from(f: squeal::ir::WindowFrame) -> Self {
        WindowFrame {
            units: match f.units {
                squeal::ir::FrameUnits::Rows => FrameUnits::Rows,
                squeal::ir::FrameUnits::Range => FrameUnits::Range,
            },
            start: Box::new((*f.start).into()),
            end: Box::new((*f.end).into()),
        }
    }
}

impl From<squeal::ir::FrameBound> for FrameBound {
    fn from(b: squeal::ir::FrameBound) -> Self {
        match b {
            squeal::ir::FrameBound::UnboundedPreceding => FrameBound::UnboundedPreceding,
            squeal::ir::FrameBound::UnboundedFollowing => FrameBound::UnboundedFollowing,
            squeal::ir::FrameBound::CurrentRow => FrameBound::CurrentRow,
            squeal::ir::FrameBound::Preceding(e) => FrameBound::Preceding(Box::new((*e).into())),
            squeal::ir::FrameBound::Following(e) => FrameBound::Following(Box::new((*e).into())),
        }
    }
}

impl From<squeal::ir::SetOperationClause> for SetOperationClause {
    fn from(s: squeal::ir::SetOperationClause) -> Self {
        SetOperationClause {
            operator: match s.operator {
                squeal::ir::SetOperator::Union => SetOperator::Union,
                squeal::ir::SetOperator::UnionAll => SetOperator::UnionAll,
                squeal::ir::SetOperator::Intersect => SetOperator::Intersect,
                squeal::ir::SetOperator::Except => SetOperator::Except,
            },
            select: Box::new((*s.select).into()),
        }
    }
}
