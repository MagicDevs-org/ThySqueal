pub mod ddl;
pub mod dml;
pub mod expr;
pub mod select;
pub mod utils;

pub use ddl::parse_data_type_from_rule;

use crate::engines::mysql::ast::{CallStmt, SqlStmt, VariableDeclaration};
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::squeal::exec::ParseResult;
use crate::squeal::ir::Squeal;
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "engines/mysql/mysql.pest"]
pub struct SqlParser;

pub fn parse_to_squeal(sql: &str) -> ParseResult<Squeal> {
    let mysql_stmt = parse(sql)?;
    Ok(Squeal::from(mysql_stmt))
}

pub fn parse(sql: &str) -> SqlResult<SqlStmt> {
    let pairs = SqlParser::parse(Rule::statement, sql)
        .map_err(|e| SqlError::Parse(format!("Pest error: {}", e)))?;

    for pair in pairs {
        if pair.as_rule() == Rule::statement {
            let inner = pair.into_inner().next().unwrap();
            let mut stmt = match inner.as_rule() {
                Rule::select_stmt => select::parse_select(inner),
                Rule::insert_stmt => dml::parse_insert(inner),
                Rule::update_stmt => dml::parse_update(inner),
                Rule::delete_stmt => dml::parse_delete(inner),
                Rule::create_table_stmt => ddl::parse_create_table(inner),
                Rule::create_database_stmt => ddl::parse_create_database(inner),
                Rule::create_trigger_stmt => ddl::parse_create_trigger(inner),
                Rule::create_materialized_view_stmt => ddl::parse_create_materialized_view(inner),
                Rule::create_view_stmt => ddl::parse_create_view(inner),
                Rule::alter_view_stmt => ddl::parse_alter_view(inner),
                Rule::drop_view_stmt => ddl::parse_drop_view(inner),
                Rule::create_procedure_stmt => ddl::parse_create_procedure(inner),
                Rule::drop_procedure_stmt => ddl::parse_drop_procedure(inner),
                Rule::create_function_stmt => ddl::parse_create_function(inner),
                Rule::drop_function_stmt => ddl::parse_drop_function(inner),
                Rule::call_stmt => {
                    let call_inner: Vec<_> = inner.into_inner().collect();
                    let name = call_inner
                        .first()
                        .map(|p| p.as_str().trim().to_string())
                        .ok_or_else(|| SqlError::Parse("Missing procedure name".to_string()))?;

                    let args = if call_inner.len() > 1 {
                        let expr_list = &call_inner[1];
                        if expr_list.as_rule() == Rule::expression_list {
                            expr_list
                                .clone()
                                .into_inner()
                                .map(|p| expr::parse_expression(p).map(|e| e.into()))
                                .collect::<SqlResult<Vec<_>>>()?
                        } else {
                            vec![]
                        }
                    } else {
                        vec![]
                    };

                    Ok(SqlStmt::Call(CallStmt { name, args }))
                }
                Rule::alter_table_stmt => ddl::parse_alter_table(inner),
                Rule::drop_table_stmt => ddl::parse_drop_table(inner),
                Rule::drop_database_stmt => ddl::parse_drop_database(inner),
                Rule::drop_trigger_stmt => ddl::parse_drop_trigger(inner),
                Rule::create_index_stmt => ddl::parse_create_index(inner),
                Rule::create_user_stmt => dml::parse_create_user(inner),
                Rule::drop_user_stmt => dml::parse_drop_user(inner),
                Rule::grant_stmt => dml::parse_grant(inner),
                Rule::revoke_stmt => dml::parse_revoke(inner),
                Rule::explain_stmt => {
                    let inner_select = inner
                        .into_inner()
                        .find(|p| p.as_rule() == Rule::select_stmt_inner)
                        .ok_or_else(|| SqlError::Parse("Missing SELECT in EXPLAIN".to_string()))?;

                    let select_stmt = select::parse_select_inner(inner_select)?;
                    Ok(SqlStmt::Explain(select_stmt))
                }
                Rule::describe_stmt => {
                    let mut inner = inner.into_inner();
                    let table_name = inner
                        .find(|p| p.as_rule() == Rule::table_name)
                        .map(|p| {
                            p.into_inner()
                                .filter(|pi| pi.as_rule() == Rule::path_identifier)
                                .map(|pi| pi.as_str().trim().to_string())
                                .collect::<Vec<_>>()
                                .join(".")
                        })
                        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

                    Ok(SqlStmt::Describe(table_name))
                }
                Rule::use_stmt => {
                    let mut inner = inner.into_inner();
                    let db_name = inner
                        .find(|p| p.as_rule() == Rule::identifier)
                        .map(|p| p.as_str().trim().to_string())
                        .ok_or_else(|| SqlError::Parse("Missing database name".to_string()))?;
                    Ok(SqlStmt::Use(db_name))
                }
                Rule::search_stmt => dml::parse_search(inner),
                Rule::show_stmt => dml::parse_show(inner),
                Rule::prepare_stmt => dml::parse_prepare(inner),
                Rule::execute_stmt => dml::parse_execute(inner),
                Rule::deallocate_stmt => dml::parse_deallocate(inner),
                Rule::begin_stmt => Ok(SqlStmt::Begin),
                Rule::begin_end_block => {
                    let inner: Vec<_> = inner.into_inner().collect();
                    let declarations: Vec<VariableDeclaration> = inner
                        .iter()
                        .filter(|p| p.as_rule() == Rule::declare_stmt)
                        .map(|p| {
                            let mut decl_inner = p.clone().into_inner();
                            let name = decl_inner
                                .next()
                                .map(|n| n.as_str().trim().to_string())
                                .ok_or_else(|| {
                                    SqlError::Parse("Missing declare name".to_string())
                                })?;
                            let data_type_pair = decl_inner.next().ok_or_else(|| {
                                SqlError::Parse("Missing declare type".to_string())
                            })?;
                            let data_type =
                                crate::engines::mysql::parser::ddl::parse_data_type_from_rule(
                                    data_type_pair,
                                )?;
                            let default_value = if decl_inner.peek().map(|p| p.as_rule())
                                == Some(Rule::KW_DEFAULT)
                            {
                                let _ = decl_inner.next();
                                let expr_pair = decl_inner.next();
                                expr_pair
                                    .map(|e| expr::parse_expression(e).map(|ex| ex.into()))
                                    .transpose()?
                            } else {
                                None
                            };
                            Ok(VariableDeclaration {
                                name,
                                data_type,
                                default_value,
                            })
                        })
                        .collect::<SqlResult<Vec<_>>>()?;

                    let stmts: Vec<SqlStmt> = inner
                        .into_iter()
                        .filter(|p| p.as_rule() == Rule::statement)
                        .map(|p| {
                            let stmt_inner = p.into_inner().next().unwrap();
                            parse_statement(stmt_inner)
                        })
                        .collect::<SqlResult<Vec<_>>>()?;
                    Ok(SqlStmt::BeginEndBlock(declarations, stmts))
                }
                Rule::commit_stmt => Ok(SqlStmt::Commit),
                Rule::rollback_stmt => dml::parse_rollback(inner),
                Rule::savepoint_stmt => dml::parse_savepoint(inner),
                _ => {
                    return Err(SqlError::Parse(format!(
                        "Unsupported statement: {:?}",
                        inner.as_rule()
                    )));
                }
            }?;

            stmt.resolve_placeholders();
            return Ok(stmt);
        }
    }

    Err(SqlError::Parse("No statement found".to_string()))
}

fn parse_statement(inner: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    match inner.as_rule() {
        Rule::select_stmt => select::parse_select(inner),
        Rule::insert_stmt => dml::parse_insert(inner),
        Rule::update_stmt => dml::parse_update(inner),
        Rule::delete_stmt => dml::parse_delete(inner),
        Rule::create_table_stmt => ddl::parse_create_table(inner),
        Rule::create_database_stmt => ddl::parse_create_database(inner),
        Rule::create_trigger_stmt => ddl::parse_create_trigger(inner),
        Rule::create_materialized_view_stmt => ddl::parse_create_materialized_view(inner),
        Rule::create_view_stmt => ddl::parse_create_view(inner),
        Rule::alter_view_stmt => ddl::parse_alter_view(inner),
        Rule::drop_view_stmt => ddl::parse_drop_view(inner),
        Rule::create_index_stmt => ddl::parse_create_index(inner),
        Rule::create_user_stmt => dml::parse_create_user(inner),
        Rule::drop_user_stmt => dml::parse_drop_user(inner),
        Rule::grant_stmt => dml::parse_grant(inner),
        Rule::revoke_stmt => dml::parse_revoke(inner),
        Rule::explain_stmt => {
            let inner_select = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::select_stmt_inner)
                .ok_or_else(|| SqlError::Parse("Missing SELECT in EXPLAIN".to_string()))?;
            let select_stmt = select::parse_select_inner(inner_select)?;
            Ok(SqlStmt::Explain(select_stmt))
        }
        Rule::describe_stmt => {
            let mut inner = inner.into_inner();
            let table_name = inner
                .find(|p| p.as_rule() == Rule::table_name)
                .map(|p| {
                    p.into_inner()
                        .filter(|pi| pi.as_rule() == Rule::path_identifier)
                        .map(|pi| pi.as_str().trim().to_string())
                        .collect::<Vec<_>>()
                        .join(".")
                })
                .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
            Ok(SqlStmt::Describe(table_name))
        }
        Rule::use_stmt => {
            let mut inner = inner.into_inner();
            let db_name = inner
                .find(|p| p.as_rule() == Rule::identifier)
                .map(|p| p.as_str().trim().to_string())
                .ok_or_else(|| SqlError::Parse("Missing database name".to_string()))?;
            Ok(SqlStmt::Use(db_name))
        }
        Rule::search_stmt => dml::parse_search(inner),
        Rule::show_stmt => dml::parse_show(inner),
        Rule::prepare_stmt => dml::parse_prepare(inner),
        Rule::execute_stmt => dml::parse_execute(inner),
        Rule::deallocate_stmt => dml::parse_deallocate(inner),
        Rule::begin_stmt => Ok(SqlStmt::Begin),
        Rule::begin_end_block => {
            let inner: Vec<_> = inner.into_inner().collect();
            let declarations: Vec<VariableDeclaration> = inner
                .iter()
                .filter(|p| p.as_rule() == Rule::declare_stmt)
                .map(|p| {
                    let mut decl_inner = p.clone().into_inner();
                    let name = decl_inner
                        .next()
                        .map(|n| n.as_str().trim().to_string())
                        .ok_or_else(|| SqlError::Parse("Missing declare name".to_string()))?;
                    let data_type_pair = decl_inner
                        .next()
                        .ok_or_else(|| SqlError::Parse("Missing declare type".to_string()))?;
                    let data_type = crate::engines::mysql::parser::ddl::parse_data_type_from_rule(
                        data_type_pair,
                    )?;
                    let default_value =
                        if decl_inner.peek().map(|p| p.as_rule()) == Some(Rule::KW_DEFAULT) {
                            let _ = decl_inner.next();
                            let expr_pair = decl_inner.next();
                            expr_pair
                                .map(|e| expr::parse_expression(e).map(|ex| ex.into()))
                                .transpose()?
                        } else {
                            None
                        };
                    Ok(VariableDeclaration {
                        name,
                        data_type,
                        default_value,
                    })
                })
                .collect::<SqlResult<Vec<_>>>()?;

            let stmts: Vec<SqlStmt> = inner
                .into_iter()
                .filter(|p| p.as_rule() == Rule::statement)
                .map(|p| {
                    let stmt_inner = p.into_inner().next().unwrap();
                    parse_statement(stmt_inner)
                })
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(SqlStmt::BeginEndBlock(declarations, stmts))
        }
        Rule::commit_stmt => Ok(SqlStmt::Commit),
        Rule::rollback_stmt => dml::parse_rollback(inner),
        Rule::savepoint_stmt => dml::parse_savepoint(inner),
        _ => Err(SqlError::Parse(format!(
            "Unsupported statement in block: {:?}",
            inner.as_rule()
        ))),
    }
}
