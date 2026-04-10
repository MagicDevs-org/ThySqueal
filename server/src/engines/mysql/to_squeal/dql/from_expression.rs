use crate::engines::mysql::ast;
use crate::squeal::ir::expr::*;

impl From<ast::Expression> for Expression {
    fn from(e: ast::Expression) -> Self {
        match e {
            ast::Expression::Literal(v) => Expression::Literal(v),
            ast::Expression::Placeholder(i) => Expression::Placeholder(i),
            ast::Expression::Column(c) => Expression::Column(c),
            ast::Expression::BinaryOp(l, op, r) => Expression::BinaryOp(
                Box::new((*l).into()),
                match op {
                    ast::BinaryOp::Add => BinaryOp::Add,
                    ast::BinaryOp::Sub => BinaryOp::Sub,
                    ast::BinaryOp::Mul => BinaryOp::Mul,
                    ast::BinaryOp::Div => BinaryOp::Div,
                },
                Box::new((*r).into()),
            ),
            ast::Expression::FunctionCall(f) => Expression::FunctionCall(FunctionCall {
                name: match f.name {
                    ast::AggregateType::Count => AggregateType::Count,
                    ast::AggregateType::Sum => AggregateType::Sum,
                    ast::AggregateType::Avg => AggregateType::Avg,
                    ast::AggregateType::Min => AggregateType::Min,
                    ast::AggregateType::Max => AggregateType::Max,
                    ast::AggregateType::GroupConcat => AggregateType::GroupConcat,
                    ast::AggregateType::JsonArrayAgg => AggregateType::JsonArrayAgg,
                    ast::AggregateType::JsonObjectAgg => AggregateType::JsonObjectAgg,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
            }),
            ast::Expression::ScalarFunc(f) => Expression::ScalarFunc(ScalarFunction {
                name: match f.name {
                    ast::ScalarFuncType::Lower => ScalarFuncType::Lower,
                    ast::ScalarFuncType::Upper => ScalarFuncType::Upper,
                    ast::ScalarFuncType::Length => ScalarFuncType::Length,
                    ast::ScalarFuncType::Abs => ScalarFuncType::Abs,
                    ast::ScalarFuncType::Now => ScalarFuncType::Now,
                    ast::ScalarFuncType::Concat => ScalarFuncType::Concat,
                    ast::ScalarFuncType::Coalesce => ScalarFuncType::Coalesce,
                    ast::ScalarFuncType::Replace => ScalarFuncType::Replace,
                    ast::ScalarFuncType::IfNull => ScalarFuncType::IfNull,
                    ast::ScalarFuncType::If => ScalarFuncType::If,
                    ast::ScalarFuncType::DateDiff => ScalarFuncType::DateDiff,
                    ast::ScalarFuncType::DateFormat => ScalarFuncType::DateFormat,
                    ast::ScalarFuncType::Md5 => ScalarFuncType::Md5,
                    ast::ScalarFuncType::Sha2 => ScalarFuncType::Sha2,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
            }),
            ast::Expression::WindowFunc(f) => Expression::WindowFunc(WindowFunction {
                func_type: match f.func_type {
                    ast::WindowFuncType::RowNumber => WindowFuncType::RowNumber,
                    ast::WindowFuncType::Rank => WindowFuncType::Rank,
                    ast::WindowFuncType::DenseRank => WindowFuncType::DenseRank,
                    ast::WindowFuncType::Ntile => WindowFuncType::Ntile,
                    ast::WindowFuncType::PercentRank => WindowFuncType::PercentRank,
                    ast::WindowFuncType::CumeDist => WindowFuncType::CumeDist,
                    ast::WindowFuncType::FirstValue => WindowFuncType::FirstValue,
                    ast::WindowFuncType::LastValue => WindowFuncType::LastValue,
                    ast::WindowFuncType::NthValue => WindowFuncType::NthValue,
                    ast::WindowFuncType::Lag => WindowFuncType::Lag,
                    ast::WindowFuncType::Lead => WindowFuncType::Lead,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
                partition_by: f.partition_by.into_iter().map(|e| e.into()).collect(),
                order_by: f.order_by.into_iter().map(|o| o.into()).collect(),
                frame: f.frame.map(|f| Box::new((*f).into())),
            }),
            ast::Expression::Star => Expression::Star,
            ast::Expression::Variable(v) => Expression::Variable(Variable {
                name: v.name,
                is_system: v.is_system,
                scope: match v.scope {
                    ast::VariableScope::Global => VariableScope::Global,
                    ast::VariableScope::Session => VariableScope::Session,
                    ast::VariableScope::User => VariableScope::User,
                },
            }),
            ast::Expression::Subquery(s) => Expression::Subquery(Box::new((*s).into())),
            ast::Expression::UnaryNot(e) => Expression::UnaryNot(Box::new((*e).into())),
        }
    }
}
