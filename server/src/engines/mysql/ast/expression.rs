use super::statements::SelectStmt;
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
    Subquery(Box<SelectStmt>),
    UnaryNot(Box<Expression>),
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

impl Expression {
    pub fn resolve_placeholders(&mut self, counter: &mut usize) {
        match self {
            Expression::Placeholder(i) => {
                if *i == 0 {
                    *counter += 1;
                    *i = *counter;
                }
            }
            Expression::BinaryOp(l, _, r) => {
                l.resolve_placeholders(counter);
                r.resolve_placeholders(counter);
            }
            Expression::FunctionCall(fc) => {
                for arg in &mut fc.args {
                    arg.resolve_placeholders(counter);
                }
            }
            Expression::ScalarFunc(sf) => {
                for arg in &mut sf.args {
                    arg.resolve_placeholders(counter);
                }
            }
            Expression::WindowFunc(wf) => {
                for arg in &mut wf.args {
                    arg.resolve_placeholders(counter);
                }
                for expr in &mut wf.partition_by {
                    expr.resolve_placeholders(counter);
                }
                for item in &mut wf.order_by {
                    item.expr.resolve_placeholders(counter);
                }
            }
            Expression::Subquery(s) => {
                s.resolve_placeholders(counter);
            }
            _ => {}
        }
    }

    #[allow(dead_code)]
    pub fn to_sql(&self) -> String {
        match self {
            Expression::Literal(v) => v.to_sql(),
            Expression::Placeholder(i) => format!("${}", i),
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
            Expression::FunctionCall(fc) => {
                let args: Vec<String> = fc.args.iter().map(|a| a.to_sql()).collect();
                format!("{}({})", fc.name.to_sql(), args.join(", "))
            }
            Expression::ScalarFunc(sf) => {
                let args: Vec<String> = sf.args.iter().map(|a| a.to_sql()).collect();
                format!("{}({})", sf.name.to_sql(), args.join(", "))
            }
            Expression::WindowFunc(wf) => {
                let args: Vec<String> = wf.args.iter().map(|a| a.to_sql()).collect();
                let over = Self::window_spec_to_sql(&wf.partition_by, &wf.order_by, &wf.frame);
                format!(
                    "{}({}) OVER ({})",
                    wf.func_type.to_sql(),
                    args.join(", "),
                    over
                )
            }
            Expression::Star => "*".to_string(),
            Expression::Subquery(_) => "(SELECT ...)".to_string(),
            Expression::UnaryNot(e) => format!("NOT ({})", e.to_sql()),
        }
    }
}

impl Expression {
    fn window_spec_to_sql(
        partition_by: &[Expression],
        order_by: &[WindowOrderByItem],
        frame: &Option<Box<WindowFrame>>,
    ) -> String {
        let mut parts = Vec::new();

        if !partition_by.is_empty() {
            let cols: Vec<String> = partition_by.iter().map(|e| e.to_sql()).collect();
            parts.push(format!("PARTITION BY {}", cols.join(", ")));
        }

        if !order_by.is_empty() {
            let items: Vec<String> = order_by
                .iter()
                .map(|item| {
                    let order = item.order.to_string();
                    format!("{} {}", item.expr.to_sql(), order)
                })
                .collect();
            parts.push(format!("ORDER BY {}", items.join(", ")));
        }

        if let Some(f) = frame {
            let units = match f.units {
                FrameUnits::Rows => "ROWS",
                FrameUnits::Range => "RANGE",
            };
            let start = Self::frame_bound_to_sql(&f.start);
            let end = Self::frame_bound_to_sql(&f.end);
            if start == "CURRENT ROW" && end == "CURRENT ROW" {
                parts.push(format!(
                    "{} BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                    units
                ));
            } else {
                parts.push(format!("{} BETWEEN {} AND {}", units, start, end));
            }
        }

        parts.join(" ")
    }

    fn frame_bound_to_sql(bound: &FrameBound) -> &'static str {
        match bound {
            FrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING",
            FrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING",
            FrameBound::CurrentRow => "CURRENT ROW",
            FrameBound::Preceding(_) => "n PRECEDING",
            FrameBound::Following(_) => "n FOLLOWING",
        }
    }
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

impl ScalarFuncType {
    pub fn to_sql(&self) -> &str {
        match self {
            ScalarFuncType::Lower => "LOWER",
            ScalarFuncType::Upper => "UPPER",
            ScalarFuncType::Length => "LENGTH",
            ScalarFuncType::Abs => "ABS",
            ScalarFuncType::Now => "NOW",
            ScalarFuncType::Concat => "CONCAT",
            ScalarFuncType::Coalesce => "COALESCE",
            ScalarFuncType::Replace => "REPLACE",
            ScalarFuncType::IfNull => "IFNULL",
            ScalarFuncType::If => "IF",
            ScalarFuncType::DateDiff => "DATEDIFF",
            ScalarFuncType::DateFormat => "DATE_FORMAT",
            ScalarFuncType::Md5 => "MD5",
            ScalarFuncType::Sha2 => "SHA2",
        }
    }
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

impl AggregateType {
    pub fn to_sql(&self) -> &str {
        match self {
            AggregateType::Count => "COUNT",
            AggregateType::Sum => "SUM",
            AggregateType::Avg => "AVG",
            AggregateType::Min => "MIN",
            AggregateType::Max => "MAX",
            AggregateType::GroupConcat => "GROUP_CONCAT",
            AggregateType::JsonArrayAgg => "JSON_ARRAYAGG",
            AggregateType::JsonObjectAgg => "JSON_OBJECTAGG",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl BinaryOp {
    #[allow(dead_code)]
    pub fn to_sql(&self) -> &str {
        match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowFunction {
    pub func_type: WindowFuncType,
    pub args: Vec<Expression>,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<WindowOrderByItem>,
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

impl WindowFuncType {
    pub fn to_sql(&self) -> &str {
        match self {
            WindowFuncType::RowNumber => "ROW_NUMBER",
            WindowFuncType::Rank => "RANK",
            WindowFuncType::DenseRank => "DENSE_RANK",
            WindowFuncType::Ntile => "NTILE",
            WindowFuncType::PercentRank => "PERCENT_RANK",
            WindowFuncType::CumeDist => "CUME_DIST",
            WindowFuncType::FirstValue => "FIRST_VALUE",
            WindowFuncType::LastValue => "LAST_VALUE",
            WindowFuncType::NthValue => "NTH_VALUE",
            WindowFuncType::Lag => "LAG",
            WindowFuncType::Lead => "LEAD",
        }
    }
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowOrderByItem {
    pub expr: Expression,
    pub order: super::statements::Order,
}
