pub mod case_when;
pub mod condition;
pub mod functions;
pub mod literal;

use crate::engines::mysql::ast::{BinaryOp, Expression, SqlStmt};
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::engines::mysql::parser::Rule;

pub use case_when::parse_case_when;
pub use condition::{parse_condition, parse_where_clause};
pub use functions::{parse_aggregate, parse_scalar_func, parse_window_function};
pub use literal::parse_literal;

pub fn parse_any_expression(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    match pair.as_rule() {
        Rule::expression => parse_expression(pair),
        Rule::term => parse_term(pair),
        Rule::factor => parse_factor(pair),
        Rule::literal
        | Rule::string_literal
        | Rule::number_literal
        | Rule::boolean_literal
        | Rule::KW_NULL => Ok(Expression::Literal(parse_literal(pair)?)),
        Rule::variable => parse_variable(pair),
        Rule::placeholder => parse_placeholder(pair),
        Rule::aggregate_func => parse_aggregate(pair),
        Rule::scalar_func => parse_scalar_func(pair),
        _ => Err(SqlError::Parse(format!(
            "Unexpected rule for expression: {:?}",
            pair.as_rule()
        ))),
    }
}

pub fn parse_variable(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let var_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty variable".to_string()))?;

    match var_pair.as_rule() {
        Rule::system_variable => {
            let mut sys_inner = var_pair.into_inner();
            let first = sys_inner
                .next()
                .ok_or_else(|| SqlError::Parse("Empty system variable".to_string()))?;

            let (scope, name_pair) = if first.as_rule() == Rule::identifier {
                (crate::engines::mysql::ast::VariableScope::Session, first)
            } else {
                let scope = match first.as_str().to_uppercase().as_str() {
                    "GLOBAL." => crate::engines::mysql::ast::VariableScope::Global,
                    "SESSION." => crate::engines::mysql::ast::VariableScope::Session,
                    _ => crate::engines::mysql::ast::VariableScope::Session,
                };
                let name = sys_inner
                    .next()
                    .ok_or_else(|| SqlError::Parse("Missing system variable name".to_string()))?;
                (scope, name)
            };

            Ok(Expression::Variable(crate::engines::mysql::ast::Variable {
                name: name_pair.as_str().to_string(),
                is_system: true,
                scope,
            }))
        }
        Rule::session_variable => {
            let mut sess_inner = var_pair.into_inner();
            let name = sess_inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing session variable name".to_string()))?;
            Ok(Expression::Variable(crate::engines::mysql::ast::Variable {
                name: name.as_str().to_string(),
                is_system: false,
                scope: crate::engines::mysql::ast::VariableScope::User,
            }))
        }
        _ => Err(SqlError::Parse(format!(
            "Unexpected variable rule: {:?}",
            var_pair.as_rule()
        ))),
    }
}

pub fn parse_expression(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty expression/term".to_string()))?;

    let mut left = parse_any_expression(first)?;

    while let Some(op_pair) = inner.next() {
        let op_str = op_pair.as_str().trim();
        let op = match op_str {
            "+" => BinaryOp::Add,
            "-" => BinaryOp::Sub,
            _ => {
                return Err(SqlError::Parse(format!(
                    "Unsupported binary operator in expression: '{}'",
                    op_str
                )));
            }
        };
        let right_pair = inner
            .next()
            .ok_or_else(|| SqlError::Parse("Missing right term".to_string()))?;
        let right = parse_term(right_pair)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }

    Ok(left)
}

pub fn parse_term(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty expression/term".to_string()))?;

    let mut left = parse_any_expression(first)?;

    while let Some(op_pair) = inner.next() {
        let op_str = op_pair.as_str().trim();
        let op = match op_str {
            "*" => BinaryOp::Mul,
            "/" => BinaryOp::Div,
            "%" => {
                return Err(SqlError::Parse(
                    "Modulo operator not yet supported".to_string(),
                ));
            }
            _ => {
                return Err(SqlError::Parse(format!(
                    "Unsupported binary operator in term: '{}'",
                    op_str
                )));
            }
        };
        let right_pair = inner
            .next()
            .ok_or_else(|| SqlError::Parse("Missing right factor".to_string()))?;
        let right = parse_factor(right_pair)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }

    Ok(left)
}

pub fn parse_factor(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.clone().into_inner();
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty factor".to_string()))?;

    match first.as_rule() {
        Rule::aggregate_func => parse_aggregate(first),
        Rule::scalar_func => parse_scalar_func(first),
        Rule::window_func => parse_window_function(first),
        Rule::case_when => parse_case_when(first),
        Rule::literal
        | Rule::string_literal
        | Rule::number_literal
        | Rule::boolean_literal
        | Rule::KW_NULL => Ok(Expression::Literal(parse_literal(first)?)),
        Rule::variable => parse_variable(first),
        Rule::placeholder => parse_placeholder(first),
        Rule::column_ref => {
            // If it matches KW_NULL exactly, it might be a mistake in rule precedence.
            if first.as_str().to_uppercase() == "NULL" {
                return Ok(Expression::Literal(crate::storage::Value::Null));
            }
            let parts: Vec<String> = first
                .into_inner()
                .filter(|p| p.as_rule() == Rule::path_identifier)
                .map(|p| p.as_str().trim().to_string())
                .collect();
            Ok(Expression::Column(parts.join(".")))
        }

        Rule::select_stmt | Rule::select_stmt_inner => {
            let stmt = super::select::parse_select(first)?;
            if let SqlStmt::Select(s) = stmt {
                Ok(Expression::Subquery(Box::new(s)))
            } else {
                Err(SqlError::Parse(
                    "Expected SELECT statement in subquery".to_string(),
                ))
            }
        }
        Rule::expression => parse_any_expression(first),
        Rule::KW_NOT => {
            let next_factor = inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing factor after NOT".to_string()))?;
            let negated = parse_factor(next_factor)?;
            Ok(Expression::UnaryNot(Box::new(negated)))
        }
        _ => {
            if first.as_str().starts_with('(')
                && let Some(inner_pair) = first.clone().into_inner().find(|p| {
                    p.as_rule() == Rule::select_stmt || p.as_rule() == Rule::select_stmt_inner
                })
            {
                let stmt = super::select::parse_select(inner_pair)?;
                if let SqlStmt::Select(s) = stmt {
                    return Ok(Expression::Subquery(Box::new(s)));
                }
            }

            Err(SqlError::Parse(format!(
                "Unsupported factor rule: {:?}",
                first.as_rule()
            )))
        }
    }
}

pub fn parse_placeholder(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let s = pair.as_str();
    if s == "?" {
        Ok(Expression::Placeholder(0))
    } else if let Some(idx_str) = s.strip_prefix('$') {
        let idx = idx_str
            .parse::<usize>()
            .map_err(|_| SqlError::Parse(format!("Invalid placeholder index: {}", s)))?;
        Ok(Expression::Placeholder(idx))
    } else {
        Err(SqlError::Parse(format!("Invalid placeholder: {}", s)))
    }
}
