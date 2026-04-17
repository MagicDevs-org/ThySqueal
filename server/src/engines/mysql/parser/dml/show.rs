use crate::engines::mysql::ast::{ShowStmt, ShowVariant, SqlStmt};
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::engines::mysql::parser::Rule;
use pest::iterators::Pair;

pub fn parse_show(pair: Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let variant = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing SHOW variant".to_string()))?;

    let show_variant = match variant.as_rule() {
        Rule::show_tables => {
            let db_name = inner
                .find(|p| p.as_rule() == Rule::identifier)
                .map(|p| p.as_str().trim().to_string());
            ShowVariant::Tables(db_name)
        }
        Rule::show_databases => ShowVariant::Databases,
        Rule::show_columns => {
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
            ShowVariant::Columns(table_name)
        }
        Rule::show_create_table => {
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
            ShowVariant::CreateTable(table_name)
        }
        Rule::show_create_database => {
            let db_name = inner
                .find(|p| p.as_rule() == Rule::identifier)
                .map(|p| p.as_str().trim().to_string())
                .ok_or_else(|| SqlError::Parse("Missing database name".to_string()))?;
            ShowVariant::CreateDatabase(db_name)
        }
        Rule::show_index => {
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
            ShowVariant::Index(table_name)
        }
        Rule::show_variables => {
            let like_pattern = inner
                .find(|p| p.as_rule() == Rule::string_literal)
                .map(|p| p.as_str().trim().to_string());
            ShowVariant::Variables(like_pattern)
        }
        Rule::show_status => {
            let like_pattern = inner
                .find(|p| p.as_rule() == Rule::string_literal)
                .map(|p| p.as_str().trim().to_string());
            ShowVariant::Status(like_pattern)
        }
        Rule::show_processlist => ShowVariant::Processlist,
        _ => {
            return Err(SqlError::Parse(format!(
                "Unknown SHOW variant: {:?}",
                variant.as_rule()
            )));
        }
    };

    Ok(SqlStmt::Show(ShowStmt {
        variant: show_variant,
    }))
}
