use super::super::ast::{CreateTableStmt, DropTableStmt, SqlStmt};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use super::utils::expect_identifier;
use crate::storage::{Column, DataType};

pub fn parse_create_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let name = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
    let column_defs = inner
        .find(|p| p.as_rule() == Rule::column_defs)
        .ok_or_else(|| SqlError::Parse("Missing column definitions".to_string()))?
        .into_inner();

    let mut columns = Vec::new();
    for col_def in column_defs {
        if col_def.as_rule() != Rule::column_def {
            continue;
        }
        let mut col_inner = col_def.into_inner();
        let col_name = expect_identifier(
            col_inner.find(|p| p.as_rule() == Rule::identifier),
            "column name",
        )?;
        let type_str = col_inner
            .find(|p| p.as_rule() == Rule::data_type)
            .ok_or_else(|| SqlError::Parse("Missing column type".to_string()))?
            .as_str()
            .to_uppercase();
        columns.push(Column {
            name: col_name,
            data_type: DataType::from_str(&type_str),
        });
    }

    Ok(SqlStmt::CreateTable(CreateTableStmt { name, columns }))
}

pub fn parse_drop_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();
    let name = inner
        .filter(|p| p.as_rule() == Rule::table_name)
        .last()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
    Ok(SqlStmt::DropTable(DropTableStmt { name }))
}
