use crate::storage::{Column, DataType};
use super::super::ast::{CreateIndexStmt, CreateTableStmt, DropTableStmt, SqlStmt, IndexType};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use super::utils::expect_identifier;

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
        let col_name = expect_identifier(col_inner.find(|p| p.as_rule() == Rule::identifier), "column name")?;
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

pub fn parse_create_index(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();
    
    let mut unique = false;
    let mut index_name = None;
    let mut table = None;
    let mut expressions = Vec::new();
    let mut index_type = IndexType::BTree;
    let mut where_clause = None;

    for p in inner {
        match p.as_rule() {
            Rule::unique => unique = true,
            Rule::identifier => {
                if index_name.is_none() {
                    index_name = Some(p.as_str().trim().to_string());
                }
            }
            Rule::table_name => table = Some(p.as_str().trim().to_string()),
            Rule::index_expression_list => {
                for expr_pair in p.into_inner() {
                    if expr_pair.as_rule() == Rule::expression {
                        expressions.push(super::expr::parse_expression(expr_pair)?);
                    }
                }
            }
            Rule::index_type_clause => {
                let type_inner = p.into_inner().find(|it| it.as_rule() == Rule::index_type)
                    .ok_or_else(|| SqlError::Parse("Missing index type".to_string()))?;
                if type_inner.as_str().to_uppercase() == "HASH" {
                    index_type = IndexType::Hash;
                }
            }
            Rule::where_clause => {
                where_clause = Some(super::expr::parse_where_clause(p)?);
            }
            _ => {}
        }
    }

    let name = index_name.ok_or_else(|| SqlError::Parse("Missing index name".to_string()))?;
    let table = table.ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
    if expressions.is_empty() {
        return Err(SqlError::Parse("Index must have at least one expression".to_string()));
    }

    Ok(SqlStmt::CreateIndex(CreateIndexStmt { 
        name, 
        table, 
        expressions, 
        unique, 
        index_type,
        where_clause
    }))
}
