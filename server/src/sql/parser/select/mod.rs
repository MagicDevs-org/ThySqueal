pub mod clauses;
pub mod columns;
pub mod joins;

use super::super::ast::{SelectStmt, SetOperationClause, SetOperator, SqlStmt};
use super::super::error::{SqlError, SqlResult};
use super::expr::parse_condition;
use crate::sql::parser::Rule;
pub use clauses::*;
pub use columns::*;
pub use joins::*;

pub fn parse_select(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    if pair.as_rule() == Rule::select_stmt_inner {
        let stmt = parse_select_inner(pair)?;
        return Ok(SqlStmt::Select(stmt));
    }

    let mut inner = pair.into_inner();
    let mut with_clause = None;
    let mut set_operations = Vec::new();

    let mut first = inner.next().unwrap();
    if first.as_rule() == Rule::with_clause {
        let mut recursive = false;
        let mut ctes = Vec::new();
        for cte_pair in first.into_inner() {
            if cte_pair.as_rule() == Rule::KW_RECURSIVE {
                recursive = true;
                continue;
            }
            if cte_pair.as_rule() == Rule::cte_definition {
                let mut cte_inner = cte_pair.into_inner();
                let name = cte_inner.next().unwrap().as_str().trim().to_string();
                let _ = cte_inner.next();
                let query_pair = cte_inner.next().unwrap();
                let sql_stmt = parse_select(query_pair)?;
                let query = match sql_stmt {
                    SqlStmt::Select(s) => s,
                    _ => {
                        return Err(SqlError::Parse(
                            "CTE must contain a SELECT statement".to_string(),
                        ));
                    }
                };
                ctes.push(crate::sql::ast::Cte { name, query });
            }
        }
        with_clause = Some(crate::sql::ast::WithClause { recursive, ctes });
        first = inner
            .next()
            .ok_or_else(|| SqlError::Parse("Missing SELECT after WITH".to_string()))?;
    }

    let mut stmt = parse_select_inner(first)?;
    stmt.with_clause = with_clause;

    for remaining in inner {
        if remaining.as_rule() == Rule::set_operation {
            let set_op = parse_set_operation(remaining)?;
            set_operations.push(set_op);
        }
    }

    stmt.set_operations = set_operations;
    Ok(SqlStmt::Select(stmt))
}

fn parse_set_operation(pair: pest::iterators::Pair<Rule>) -> SqlResult<SetOperationClause> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    let operator = match first.as_rule() {
        Rule::KW_UNION => {
            let mut op_inner = first.clone().into_inner();
            let union_keyword = op_inner.next().unwrap();
            if union_keyword.as_rule() == Rule::KW_UNION {
                if let Some(all_kw) = op_inner.next() {
                    if all_kw.as_rule() == Rule::all {
                        SetOperator::UnionAll
                    } else {
                        SetOperator::Union
                    }
                } else {
                    SetOperator::Union
                }
            } else {
                SetOperator::Union
            }
        }
        Rule::KW_INTERSECT => SetOperator::Intersect,
        Rule::KW_EXCEPT => SetOperator::Except,
        Rule::set_op => {
            let set_op_inner = first.into_inner().collect::<Vec<_>>();
            if let Some(op) = set_op_inner.first() {
                match op.as_rule() {
                    Rule::KW_UNION => {
                        if set_op_inner.len() > 1 && set_op_inner[1].as_rule() == Rule::all {
                            SetOperator::UnionAll
                        } else {
                            SetOperator::Union
                        }
                    }
                    Rule::KW_INTERSECT => SetOperator::Intersect,
                    Rule::KW_EXCEPT => SetOperator::Except,
                    _ => return Err(SqlError::Parse("Unknown set operator".to_string())),
                }
            } else {
                return Err(SqlError::Parse("Empty set operation".to_string()));
            }
        }
        _ => return Err(SqlError::Parse("Expected set operator".to_string())),
    };

    let select_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing SELECT after set operator".to_string()))?;

    let select = parse_select_inner(select_pair)?;

    Ok(SetOperationClause {
        operator,
        select: Box::new(select),
    })
}

pub fn parse_select_inner(pair: pest::iterators::Pair<Rule>) -> SqlResult<SelectStmt> {
    let inner = if pair.as_rule() == Rule::select_stmt_inner {
        pair.into_inner()
    } else {
        return Err(SqlError::Parse(format!(
            "Expected select_stmt_inner, got {:?}",
            pair.as_rule()
        )));
    };

    let mut distinct = false;
    let mut columns = Vec::new();
    let mut table = String::new();
    let mut table_alias = None;
    let mut joins = Vec::new();
    let mut where_clause = None;
    let mut group_by = Vec::new();
    let mut having = None;
    let mut order_by = Vec::new();
    let mut limit = None;

    for p in inner {
        match p.as_rule() {
            Rule::KW_SELECT => {}
            Rule::distinct => distinct = true,
            Rule::select_columns => columns = parse_select_columns(p)?,
            Rule::from_clause => {
                for from_p in p.clone().into_inner() {
                    match from_p.as_rule() {
                        Rule::KW_FROM => {}
                        Rule::table_name_with_alias => {
                            let mut t_inner = from_p.into_inner();
                            let table_name_rule = t_inner.next().unwrap();
                            let column_ref_rule = table_name_rule.into_inner().next().unwrap();
                            table = column_ref_rule
                                .into_inner()
                                .filter(|p| p.as_rule() == Rule::path_identifier)
                                .map(|p| p.as_str().trim().to_string())
                                .collect::<Vec<_>>()
                                .join(".");
                            if let Some(alias_pair) = t_inner.next() {
                                table_alias = Some(parse_alias(alias_pair)?);
                            }
                        }

                        Rule::join_clause => joins.push(parse_join(from_p)?),
                        Rule::where_clause => {
                            where_clause =
                                Some(parse_condition(from_p.into_inner().nth(1).unwrap())?)
                        }
                        Rule::group_by_clause => group_by = parse_group_by(from_p)?,
                        Rule::having_clause => {
                            having = Some(parse_condition(from_p.into_inner().nth(1).unwrap())?)
                        }
                        Rule::order_by_clause => order_by = parse_order_by(from_p)?,
                        Rule::limit_clause => limit = Some(parse_limit(from_p)?),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Ok(SelectStmt {
        with_clause: None,
        columns,
        table,
        table_alias,
        distinct,
        joins,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
        set_operations: Vec::new(),
    })
}
