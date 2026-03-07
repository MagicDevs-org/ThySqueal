use crate::storage::Value;
use super::super::ast::{AggregateType, BinaryOp, ComparisonOp, Condition, Expression, FunctionCall, LogicalOp};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use super::utils::expect_identifier;

pub fn parse_expression(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or_else(|| SqlError::Parse("Empty expression".to_string()))?;
    
    let mut left = parse_term(first)?;
    
    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "+" => BinaryOp::Add,
            "-" => BinaryOp::Sub,
            _ => return Err(SqlError::Parse(format!("Unsupported binary operator: {}", op_pair.as_str()))),
        };
        let right = parse_term(inner.next().ok_or_else(|| SqlError::Parse("Missing right term".to_string()))?)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }
    
    Ok(left)
}

pub fn parse_term(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or_else(|| SqlError::Parse("Empty term".to_string()))?;
    
    let mut left = parse_factor(first)?;
    
    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "*" => BinaryOp::Mul,
            "/" => BinaryOp::Div,
            _ => return Err(SqlError::Parse(format!("Unsupported binary operator: {}", op_pair.as_str()))),
        };
        let right = parse_factor(inner.next().ok_or_else(|| SqlError::Parse("Missing right factor".to_string()))?)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }
    
    Ok(left)
}

pub fn parse_factor(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let first = pair.into_inner().next().ok_or_else(|| SqlError::Parse("Empty factor".to_string()))?;
    
    match first.as_rule() {
        Rule::aggregate_func => parse_aggregate(first),
        Rule::literal => Ok(Expression::Literal(parse_literal(first)?)),
        Rule::column_ref => {
            let mut cr_inner = first.into_inner();
            let name = expect_identifier(cr_inner.find(|p| p.as_rule() == Rule::identifier), "column name")?;
            Ok(Expression::Column(name))
        }
        Rule::expression => parse_expression(first),
        Rule::KW_NOT => {
            Err(SqlError::Parse("NOT in expression factor not yet implemented".to_string()))
        }
        _ => Err(SqlError::Parse(format!("Unsupported factor rule: {:?}", first.as_rule()))),
    }
}

pub fn parse_where_clause(pair: pest::iterators::Pair<Rule>) -> SqlResult<Condition> {
    let mut inner = pair.into_inner();
    let cond_pair = inner.find(|p| p.as_rule() == Rule::condition).ok_or_else(|| SqlError::Parse("Missing condition in WHERE clause".to_string()))?;
    parse_condition(cond_pair)
}

pub fn parse_condition(pair: pest::iterators::Pair<Rule>) -> SqlResult<Condition> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or_else(|| SqlError::Parse("Empty condition".to_string()))?;

    match first.as_rule() {
        Rule::expression => {
            let left = parse_expression(first)?;
            let op_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing operator in condition".to_string()))?;
            
            match op_pair.as_rule() {
                Rule::comparison_op => {
                    let op = match op_pair.as_str().to_uppercase().as_str() {
                        "=" => ComparisonOp::Eq,
                        "!=" | "<>" => ComparisonOp::NotEq,
                        "<" => ComparisonOp::Lt,
                        ">" => ComparisonOp::Gt,
                        "<=" => ComparisonOp::LtEq,
                        ">=" => ComparisonOp::GtEq,
                        "LIKE" => ComparisonOp::Like,
                        _ => return Err(SqlError::Parse(format!("Unsupported comparison operator: {}", op_pair.as_str()))),
                    };
                    let right = parse_expression(inner.find(|p| p.as_rule() == Rule::expression).ok_or_else(|| SqlError::Parse("Missing right expression".to_string()))?)?;
                    Ok(Condition::Comparison(left, op, right))
                }
                Rule::KW_IS => {
                    let is_not;
                    if let Some(next) = inner.next() {
                        if next.as_rule() == Rule::KW_NOT {
                            is_not = true;
                            let _null = inner.next().ok_or_else(|| SqlError::Parse("Expected NULL after IS NOT".to_string()))?;
                        } else if next.as_rule() == Rule::KW_NULL {
                            is_not = false;
                        } else {
                            return Err(SqlError::Parse(format!("Expected NULL or NOT NULL after IS, got {:?}", next.as_rule())));
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
                _ => Err(SqlError::Parse(format!("Unexpected rule in condition: {:?}", op_pair.as_rule()))),
            }
        }
        Rule::condition => {
            let left = parse_condition(first)?;
            if let Some(op_pair) = inner.find(|p| p.as_rule() == Rule::logical_op) {
                let op = match op_pair.into_inner().next().ok_or_else(|| SqlError::Parse("Empty logical operator".to_string()))?.as_rule() {
                    Rule::KW_AND => LogicalOp::And,
                    Rule::KW_OR => LogicalOp::Or,
                    r => return Err(SqlError::Parse(format!("Unsupported logical operator: {:?}", r))),
                };
                let right = parse_condition(inner.find(|p| p.as_rule() == Rule::condition).ok_or_else(|| SqlError::Parse("Missing right condition".to_string()))?)?;
                Ok(Condition::Logical(Box::new(left), op, Box::new(right)))
            } else {
                Ok(left)
            }
        }
        _ => Err(SqlError::Parse(format!("Unsupported condition rule: {:?}", first.as_rule()))),
    }
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
            let kw = p.into_inner().next().ok_or_else(|| SqlError::Parse("Empty boolean literal".to_string()))?;
            Ok(Value::Bool(kw.as_rule() == Rule::KW_TRUE))
        }
        Rule::KW_NULL => Ok(Value::Null),
        _ => Err(SqlError::Parse(format!("Unknown literal rule: {:?}", p.as_rule()))),
    }
}

pub fn parse_aggregate(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let agg_type_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing aggregate type".to_string()))?;
    let agg_type = parse_aggregate_type(agg_type_pair)?;
    
    let mut args = Vec::new();
    let arg_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing aggregate argument".to_string()))?;
    match arg_pair.as_rule() {
        Rule::star => args.push(Expression::Star),
        Rule::expression => args.push(parse_expression(arg_pair)?),
        _ => return Err(SqlError::Parse(format!("Unexpected aggregate argument: {:?}", arg_pair.as_rule()))),
    }
    
    Ok(Expression::FunctionCall(FunctionCall { name: agg_type, args }))
}

pub fn parse_aggregate_type(pair: pest::iterators::Pair<Rule>) -> SqlResult<AggregateType> {
    let kw = pair.into_inner().next().ok_or_else(|| SqlError::Parse("Missing aggregate keyword".to_string()))?;
    match kw.as_rule() {
        Rule::KW_COUNT => Ok(AggregateType::Count),
        Rule::KW_SUM => Ok(AggregateType::Sum),
        Rule::KW_AVG => Ok(AggregateType::Avg),
        Rule::KW_MIN => Ok(AggregateType::Min),
        Rule::KW_MAX => Ok(AggregateType::Max),
        _ => Err(SqlError::Parse(format!("Unknown aggregate type: {:?}", kw.as_rule()))),
    }
}
