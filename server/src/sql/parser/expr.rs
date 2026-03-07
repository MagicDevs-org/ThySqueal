use super::super::ast::{
    AggregateType, BinaryOp, ComparisonOp, Condition, Expression, FunctionCall, LogicalOp,
    ScalarFuncType, ScalarFunction,
};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use crate::storage::Value;

pub fn parse_expression(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty expression".to_string()))?;

    let mut left = parse_term(first)?;

    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "+" => BinaryOp::Add,
            "-" => BinaryOp::Sub,
            _ => {
                return Err(SqlError::Parse(format!(
                    "Unsupported binary operator: {}",
                    op_pair.as_str()
                )));
            }
        };
        let right = parse_term(
            inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing right term".to_string()))?,
        )?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }

    Ok(left)
}

pub fn parse_term(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty term".to_string()))?;

    let mut left = parse_factor(first)?;

    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "*" => BinaryOp::Mul,
            "/" => BinaryOp::Div,
            _ => {
                return Err(SqlError::Parse(format!(
                    "Unsupported binary operator: {}",
                    op_pair.as_str()
                )));
            }
        };
        let right = parse_factor(
            inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing right factor".to_string()))?,
        )?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }

    Ok(left)
}

pub fn parse_factor(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let first = pair
        .into_inner()
        .next()
        .ok_or_else(|| SqlError::Parse("Empty factor".to_string()))?;

    match first.as_rule() {
        Rule::aggregate_func => parse_aggregate(first),
        Rule::scalar_func => parse_scalar_func(first),
        Rule::literal => Ok(Expression::Literal(parse_literal(first)?)),
        Rule::column_ref => {
            let parts: Vec<String> = first
                .into_inner()
                .filter(|p| p.as_rule() == Rule::identifier)
                .map(|p| p.as_str().trim().to_string())
                .collect();
            Ok(Expression::Column(parts.join(".")))
        }
        Rule::select_stmt => {
            let stmt = super::select::parse_select(first)?;
            if let super::super::ast::SqlStmt::Select(s) = stmt {
                Ok(Expression::Subquery(Box::new(s)))
            } else {
                Err(SqlError::Parse(
                    "Expected SELECT statement in subquery".to_string(),
                ))
            }
        }
        Rule::expression => parse_expression(first),
        Rule::KW_NOT => Err(SqlError::Parse(
            "NOT in expression factor not yet implemented".to_string(),
        )),
        _ => Err(SqlError::Parse(format!(
            "Unsupported factor rule: {:?}",
            first.as_rule()
        ))),
    }
}

pub fn parse_where_clause(pair: pest::iterators::Pair<Rule>) -> SqlResult<Condition> {
    let mut inner = pair.into_inner();
    let cond_pair = inner
        .find(|p| p.as_rule() == Rule::condition)
        .ok_or_else(|| SqlError::Parse("Missing condition in WHERE clause".to_string()))?;
    parse_condition(cond_pair)
}

pub fn parse_condition(pair: pest::iterators::Pair<Rule>) -> SqlResult<Condition> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty condition".to_string()))?;

    match first.as_rule() {
        Rule::expression => {
            let left = parse_expression(first)?;
            let op_pair = inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing operator in condition".to_string()))?;

            match op_pair.as_rule() {
                Rule::comparison_op => {
                    let op_str = op_pair.as_str().to_uppercase();
                    if op_str == "IN" {
                        let next_expr = inner.next().ok_or_else(|| {
                            SqlError::Parse("Expected subquery after IN".to_string())
                        })?;
                        // Find select_stmt recursively in this expression
                        let subquery = find_select_stmt(next_expr)?;
                        return Ok(Condition::InSubquery(left, Box::new(subquery)));
                    }

                    let op = match op_str.as_str() {
                        "=" => ComparisonOp::Eq,
                        "!=" | "<>" => ComparisonOp::NotEq,
                        "<" => ComparisonOp::Lt,
                        ">" => ComparisonOp::Gt,
                        "<=" => ComparisonOp::LtEq,
                        ">=" => ComparisonOp::GtEq,
                        "LIKE" => ComparisonOp::Like,
                        _ => {
                            return Err(SqlError::Parse(format!(
                                "Unsupported comparison operator: {}",
                                op_pair.as_str()
                            )));
                        }
                    };
                    let right = parse_expression(
                        inner
                            .find(|p| p.as_rule() == Rule::expression)
                            .ok_or_else(|| {
                                SqlError::Parse("Missing right expression".to_string())
                            })?,
                    )?;
                    Ok(Condition::Comparison(left, op, right))
                }
                Rule::KW_IS => {
                    let is_not;
                    if let Some(next) = inner.next() {
                        if next.as_rule() == Rule::KW_NOT {
                            is_not = true;
                            let _null = inner.next().ok_or_else(|| {
                                SqlError::Parse("Expected NULL after IS NOT".to_string())
                            })?;
                        } else if next.as_rule() == Rule::KW_NULL {
                            is_not = false;
                        } else {
                            return Err(SqlError::Parse(format!(
                                "Expected NULL or NOT NULL after IS, got {:?}",
                                next.as_rule()
                            )));
                        }
                    } else {
                        return Err(SqlError::Parse("Missing token after IS".to_string()));
                    }

                    if is_not {
                        Ok(Condition::IsNotNull(left))
                    } else {
                        Ok(Condition::IsNull(left))
                    }
                }
                _ => Err(SqlError::Parse(format!(
                    "Unexpected rule in condition: {:?}",
                    op_pair.as_rule()
                ))),
            }
        }
        Rule::condition => {
            let left = parse_condition(first)?;
            if let Some(op_pair) = inner.find(|p| p.as_rule() == Rule::logical_op) {
                let op = match op_pair
                    .into_inner()
                    .next()
                    .ok_or_else(|| SqlError::Parse("Empty logical operator".to_string()))?
                    .as_rule()
                {
                    Rule::KW_AND => LogicalOp::And,
                    Rule::KW_OR => LogicalOp::Or,
                    r => {
                        return Err(SqlError::Parse(format!(
                            "Unsupported logical operator: {:?}",
                            r
                        )));
                    }
                };
                let right = parse_condition(
                    inner
                        .find(|p| p.as_rule() == Rule::condition)
                        .ok_or_else(|| SqlError::Parse("Missing right condition".to_string()))?,
                )?;
                Ok(Condition::Logical(Box::new(left), op, Box::new(right)))
            } else {
                Ok(left)
            }
        }
        _ => Err(SqlError::Parse(format!(
            "Unsupported condition rule: {:?}",
            first.as_rule()
        ))),
    }
}

fn find_select_stmt(pair: pest::iterators::Pair<Rule>) -> SqlResult<super::super::ast::SelectStmt> {
    if pair.as_rule() == Rule::select_stmt {
        let stmt = super::select::parse_select(pair.clone())?;
        if let super::super::ast::SqlStmt::Select(s) = stmt {
            return Ok(s);
        }
    }
    for inner in pair.into_inner() {
        if let Ok(s) = find_select_stmt(inner) {
            return Ok(s);
        }
    }
    Err(SqlError::Parse(
        "Could not find SELECT statement in subquery context".to_string(),
    ))
}

pub fn parse_literal(pair: pest::iterators::Pair<Rule>) -> SqlResult<Value> {
    let mut inner = pair.clone().into_inner();
    let p = match inner.next() {
        Some(p) => p,
        None => {
            let s = pair.as_str().trim();
            if s.to_uppercase() == "NULL" {
                return Ok(Value::Null);
            }
            // If atomic literal has no children, its string value is the value
            if pair.as_rule() == Rule::string_literal {
                return Ok(Value::Text(s.trim_matches('\'').to_string()));
            }
            if pair.as_rule() == Rule::number_literal {
                return if s.contains('.') {
                    s.parse::<f64>()
                        .map(Value::Float)
                        .map_err(|_| SqlError::Parse(format!("Invalid number: {}", s)))
                } else {
                    s.parse::<i64>()
                        .map(Value::Int)
                        .map_err(|_| SqlError::Parse(format!("Invalid integer: {}", s)))
                };
            }
            return Err(SqlError::Parse(format!("Empty literal: {}", s)));
        }
    };

    match p.as_rule() {
        Rule::string_literal => Ok(Value::Text(p.as_str().trim_matches('\'').to_string())),
        Rule::number_literal => {
            let s = p.as_str().trim();
            if s.contains('.') {
                s.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| SqlError::Parse(format!("Invalid number: {}", s)))
            } else {
                s.parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| SqlError::Parse(format!("Invalid integer: {}", s)))
            }
        }
        Rule::boolean_literal => {
            let s = p.as_str().to_lowercase();
            Ok(Value::Bool(s == "true"))
        }
        Rule::KW_NULL => Ok(Value::Null),
        _ => Err(SqlError::Parse(format!(
            "Unknown literal rule: {:?}",
            p.as_rule()
        ))),
    }
}

pub fn parse_aggregate(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let agg_type_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing aggregate type".to_string()))?;
    let agg_type = parse_aggregate_type(agg_type_pair)?;

    let mut args = Vec::new();
    let arg_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing aggregate argument".to_string()))?;
    match arg_pair.as_rule() {
        Rule::star => args.push(Expression::Star),
        Rule::expression => args.push(parse_expression(arg_pair)?),
        _ => {
            return Err(SqlError::Parse(format!(
                "Unexpected aggregate argument: {:?}",
                arg_pair.as_rule()
            )));
        }
    }

    Ok(Expression::FunctionCall(FunctionCall {
        name: agg_type,
        args,
    }))
}

pub fn parse_aggregate_type(pair: pest::iterators::Pair<Rule>) -> SqlResult<AggregateType> {
    let kw = pair
        .into_inner()
        .next()
        .ok_or_else(|| SqlError::Parse("Missing aggregate keyword".to_string()))?;
    match kw.as_rule() {
        Rule::KW_COUNT => Ok(AggregateType::Count),
        Rule::KW_SUM => Ok(AggregateType::Sum),
        Rule::KW_AVG => Ok(AggregateType::Avg),
        Rule::KW_MIN => Ok(AggregateType::Min),
        Rule::KW_MAX => Ok(AggregateType::Max),
        _ => Err(SqlError::Parse(format!(
            "Unknown aggregate type: {:?}",
            kw.as_rule()
        ))),
    }
}

pub fn parse_scalar_func(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let name_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing scalar function name".to_string()))?;
    let name = parse_scalar_func_type(name_pair)?;
    let arg_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing scalar function argument".to_string()))?;
    let arg = parse_expression(arg_pair)?;

    Ok(Expression::ScalarFunc(ScalarFunction {
        name,
        arg: Box::new(arg),
    }))
}

pub fn parse_scalar_func_type(pair: pest::iterators::Pair<Rule>) -> SqlResult<ScalarFuncType> {
    let name = pair.as_str().to_uppercase();
    match name.as_str() {
        "LOWER" => Ok(ScalarFuncType::Lower),
        "UPPER" => Ok(ScalarFuncType::Upper),
        "LENGTH" => Ok(ScalarFuncType::Length),
        "ABS" => Ok(ScalarFuncType::Abs),
        _ => Err(SqlError::Parse(format!(
            "Unknown scalar function: {}",
            name
        ))),
    }
}
