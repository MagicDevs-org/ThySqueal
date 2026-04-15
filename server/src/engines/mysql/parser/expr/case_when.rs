use super::parse_expression;
use crate::engines::mysql::ast::{CaseWhen, Expression};
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::engines::mysql::parser::Rule;

pub fn parse_case_when(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let inner = pair.into_inner();

    let mut conditions = Vec::new();
    let mut else_expr = None;

    for p in inner {
        match p.as_rule() {
            Rule::when_clause => {
                let mut when_inner = p.into_inner();
                let cond = when_inner
                    .next()
                    .ok_or_else(|| SqlError::Parse("Missing WHEN condition".to_string()))?;
                let then_expr = when_inner
                    .next()
                    .ok_or_else(|| SqlError::Parse("Missing THEN expression".to_string()))?;

                let condition = parse_expression(cond)?;
                let then_value = parse_expression(then_expr)?;
                conditions.push((condition, then_value));
            }
            Rule::expression => {
                else_expr = Some(Box::new(parse_expression(p)?));
            }
            _ => {}
        }
    }

    Ok(Expression::CaseWhen(CaseWhen {
        conditions,
        else_expr,
    }))
}
