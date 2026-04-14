use crate::engines::mysql::ast::{DropDatabaseStmt, DropTableStmt, DropTriggerStmt, SqlStmt};
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::engines::mysql::parser::Rule;

pub fn parse_drop_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let table_pair = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .ok_or_else(|| SqlError::Parse("Missing table name in DROP TABLE".to_string()))?;

    let column_ref_rule = table_pair.into_inner().next().unwrap();
    let name = column_ref_rule
        .into_inner()
        .filter(|pi| pi.as_rule() == Rule::path_identifier)
        .map(|pi| pi.as_str().trim().to_string())
        .collect::<Vec<_>>()
        .join(".");

    Ok(SqlStmt::DropTable(DropTableStmt { name }))
}

pub fn parse_drop_database(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let name = inner
        .find(|p| p.as_rule() == Rule::identifier)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing database name in DROP DATABASE".to_string()))?;

    let if_exists = inner.find(|p| p.as_rule() == Rule::if_exists).is_some();

    Ok(SqlStmt::DropDatabase(DropDatabaseStmt { name, if_exists }))
}

pub fn parse_drop_trigger(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let name = inner
        .find(|p| p.as_rule() == Rule::identifier)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing trigger name in DROP TRIGGER".to_string()))?;

    Ok(SqlStmt::DropTrigger(DropTriggerStmt { name }))
}
