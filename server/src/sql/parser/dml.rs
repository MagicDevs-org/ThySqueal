use super::super::ast::{DeleteStmt, InsertStmt, SqlStmt, UpdateStmt};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use super::expr::{parse_expression, parse_literal, parse_where_clause};
use super::utils::expect_identifier;
use crate::storage::Value;

pub fn parse_insert(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();

    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

    let value_list = inner
        .clone()
        .find(|p| p.as_rule() == Rule::value_list)
        .ok_or_else(|| SqlError::Parse("Missing value list".to_string()))?;
    let values = parse_value_list(value_list)?;

    Ok(SqlStmt::Insert(InsertStmt { table, values }))
}

pub fn parse_update(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();
    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

    let set_list = inner
        .clone()
        .find(|p| p.as_rule() == Rule::set_list)
        .ok_or_else(|| SqlError::Parse("Missing SET list".to_string()))?;
    let mut assignments = Vec::new();
    for item in set_list.into_inner() {
        if item.as_rule() == Rule::set_item {
            let mut set_inner = item.into_inner();
            let col = expect_identifier(
                set_inner.find(|p| p.as_rule() == Rule::identifier),
                "column name",
            )?;
            let expr = parse_expression(
                set_inner
                    .find(|p| p.as_rule() == Rule::expression)
                    .ok_or_else(|| SqlError::Parse("Missing expression".to_string()))?,
            )?;
            assignments.push((col, expr));
        }
    }

    let where_clause = if let Some(p) = inner.clone().find(|p| p.as_rule() == Rule::where_clause) {
        Some(parse_where_clause(p)?)
    } else {
        None
    };

    Ok(SqlStmt::Update(UpdateStmt {
        table,
        assignments,
        where_clause,
    }))
}

pub fn parse_delete(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();
    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

    let where_clause = if let Some(p) = inner.clone().find(|p| p.as_rule() == Rule::where_clause) {
        Some(parse_where_clause(p)?)
    } else {
        None
    };

    Ok(SqlStmt::Delete(DeleteStmt {
        table,
        where_clause,
    }))
}

pub fn parse_value_list(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<Value>> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::literal)
        .map(parse_literal)
        .collect()
}
