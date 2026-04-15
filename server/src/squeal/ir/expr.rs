use super::stmt::Select;
use crate::storage::Value;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Literal(Value),
    Placeholder(usize),
    Column(String),
    Variable(Variable),
    BinaryOp(Box<Expression>, BinaryOp, Box<Expression>),
    FunctionCall(FunctionCall),
    ScalarFunc(ScalarFunction),
    WindowFunc(WindowFunction),
    Star,
    Subquery(Box<Select>),
    UnaryNot(Box<Expression>),
    CaseWhen(CaseWhen),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseWhen {
    pub conditions: Vec<(Expression, Expression)>,
    pub else_expr: Option<Box<Expression>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Variable {
    pub name: String,
    pub is_system: bool,
    pub scope: VariableScope,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum VariableScope {
    Global,
    Session,
    User,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: AggregateType,
    pub args: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScalarFunction {
    pub name: ScalarFuncType,
    pub args: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarFuncType {
    Lower,
    Upper,
    Length,
    Abs,
    Now,
    Concat,
    Coalesce,
    Replace,
    IfNull,
    If,
    DateDiff,
    DateFormat,
    Md5,
    Sha2,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    GroupConcat,
    JsonArrayAgg,
    JsonObjectAgg,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowFunction {
    pub func_type: WindowFuncType,
    pub args: Vec<Expression>,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<super::stmt::OrderByItem>,
    pub frame: Option<Box<WindowFrame>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowFuncType {
    RowNumber,
    Rank,
    DenseRank,
    Ntile,
    PercentRank,
    CumeDist,
    FirstValue,
    LastValue,
    NthValue,
    Lag,
    Lead,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowFrame {
    pub units: FrameUnits,
    pub start: Box<FrameBound>,
    pub end: Box<FrameBound>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FrameUnits {
    Rows,
    Range,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FrameBound {
    UnboundedPreceding,
    UnboundedFollowing,
    CurrentRow,
    Preceding(Box<Expression>),
    Following(Box<Expression>),
}

impl Expression {
    pub fn to_sql(&self) -> String {
        match self {
            Expression::Literal(v) => format!("{:?}", v),
            Expression::Placeholder(i) => format!("?{}", i),
            Expression::Column(c) => c.clone(),
            Expression::Variable(v) => {
                if v.is_system {
                    match v.scope {
                        VariableScope::Global => format!("@@global.{}", v.name),
                        VariableScope::Session => format!("@@session.{}", v.name),
                        VariableScope::User => format!("@@{}", v.name),
                    }
                } else {
                    format!("@{}", v.name)
                }
            }
            Expression::BinaryOp(l, op, r) => {
                format!("({} {} {})", l.to_sql(), op.to_sql(), r.to_sql())
            }
            Expression::FunctionCall(f) => format!("{:?}({:?})", f.name, f.args),
            Expression::ScalarFunc(f) => format!("{:?}({:?})", f.name, f.args),
            Expression::Star => "*".to_string(),
            Expression::Subquery(_) => "(subquery)".to_string(),
            Expression::UnaryNot(e) => format!("NOT ({})", e.to_sql()),
            Expression::WindowFunc(wf) => {
                format!("{:?}(...) OVER (...)", wf.func_type)
            }
            Expression::CaseWhen(cw) => {
                let mut sql = "CASE".to_string();
                for (cond, then) in &cw.conditions {
                    sql.push_str(&format!(" WHEN {} THEN {}", cond.to_sql(), then.to_sql()));
                }
                if let Some(ref else_expr) = cw.else_expr {
                    sql.push_str(&format!(" ELSE {}", else_expr.to_sql()));
                }
                sql.push_str(" END");
                sql
            }
        }
    }
}

impl BinaryOp {
    pub fn to_sql(&self) -> &str {
        match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
        }
    }
}
