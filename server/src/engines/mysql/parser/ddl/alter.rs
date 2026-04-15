use super::super::expr::literal::parse_literal;
use super::create::parse_column_def;
use crate::engines::mysql::ast::{AlterAction, AlterTableStmt, SqlStmt};
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::engines::mysql::parser::Rule;
use crate::storage::DataType;
use crate::storage::Value;

pub fn parse_alter_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let _ = inner.next();
    let _ = inner.next();

    let table_pair = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .ok_or_else(|| SqlError::Parse("Missing table name in ALTER TABLE".to_string()))?;

    let column_ref_rule = table_pair.into_inner().next().unwrap();
    let table = column_ref_rule
        .into_inner()
        .filter(|pi| pi.as_rule() == Rule::path_identifier)
        .map(|pi| pi.as_str().trim().to_string())
        .collect::<Vec<_>>()
        .join(".");

    let action_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing action in ALTER TABLE".to_string()))?;

    let action = match action_pair.as_rule() {
        Rule::alter_add_column => {
            let mut action_inner = action_pair.into_inner();
            let mut next = action_inner.next().unwrap();
            if next.as_rule() == Rule::KW_ADD {
                next = action_inner.next().unwrap();
            }
            if next.as_rule() == Rule::KW_COLUMN {
                next = action_inner.next().unwrap();
            }
            AlterAction::AddColumn(parse_column_def(next)?)
        }
        Rule::alter_drop_column => {
            let mut action_inner = action_pair.into_inner();
            let mut next = action_inner.next().unwrap();
            if next.as_rule() == Rule::KW_DROP {
                next = action_inner.next().unwrap();
            }
            if next.as_rule() == Rule::KW_COLUMN {
                next = action_inner.next().unwrap();
            }
            AlterAction::DropColumn(next.as_str().trim().to_string())
        }
        Rule::alter_rename_column => {
            let mut action_inner = action_pair.into_inner();
            let mut next = action_inner.next().unwrap();
            if next.as_rule() == Rule::KW_RENAME {
                next = action_inner.next().unwrap();
            }
            if next.as_rule() == Rule::KW_COLUMN {
                next = action_inner.next().unwrap();
            }
            let old_name = next.as_str().trim().to_string();
            let _ = action_inner.next();
            let new_name = action_inner
                .next()
                .ok_or_else(|| {
                    SqlError::Parse("Missing new column name in RENAME COLUMN".to_string())
                })?
                .as_str()
                .trim()
                .to_string();
            AlterAction::RenameColumn { old_name, new_name }
        }
        Rule::alter_rename_table => {
            let mut action_inner = action_pair.into_inner();
            let _ = action_inner.next();
            let _ = action_inner.next();
            let table_pair = action_inner.next().ok_or_else(|| {
                SqlError::Parse("Missing new table name in RENAME TABLE".to_string())
            })?;
            let column_ref_rule = table_pair.into_inner().next().unwrap();
            let new_name = column_ref_rule
                .into_inner()
                .filter(|pi| pi.as_rule() == Rule::path_identifier)
                .map(|pi| pi.as_str().trim().to_string())
                .collect::<Vec<_>>()
                .join(".");
            AlterAction::RenameTable(new_name)
        }
        Rule::alter_modify_column => {
            let mut action_inner = action_pair.into_inner();
            let mut next = action_inner.next().unwrap();
            if next.as_rule() == Rule::KW_MODIFY {
                next = action_inner.next().unwrap();
            }
            if next.as_rule() == Rule::KW_COLUMN {
                next = action_inner.next().unwrap();
            }
            let col_name = next.as_str().trim().to_string();
            let data_type_pair = action_inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing data type in MODIFY COLUMN".to_string()))?;
            let data_type = parse_data_type(data_type_pair)?;
            AlterAction::ModifyColumn {
                name: col_name,
                data_type,
            }
        }
        Rule::alter_set_default => {
            let action_inner = action_pair.into_inner();
            let mut col_name = String::new();
            let mut default_value: Option<Value> = None;
            for p in action_inner {
                match p.as_rule() {
                    Rule::identifier => col_name = p.as_str().trim().to_string(),
                    Rule::literal => default_value = Some(parse_literal(p)?),
                    Rule::KW_NULL => default_value = Some(Value::Null),
                    _ => {}
                }
            }
            AlterAction::SetDefault {
                column: col_name,
                value: default_value,
            }
        }
        Rule::alter_drop_default => {
            let action_inner = action_pair.into_inner();
            let mut col_name = String::new();
            for p in action_inner {
                if p.as_rule() == Rule::identifier {
                    col_name = p.as_str().trim().to_string();
                }
            }
            AlterAction::DropDefault { column: col_name }
        }
        Rule::alter_set_not_null => {
            let action_inner = action_pair.into_inner();
            let mut col_name = String::new();
            for p in action_inner {
                if p.as_rule() == Rule::identifier {
                    col_name = p.as_str().trim().to_string();
                }
            }
            AlterAction::SetNotNull { column: col_name }
        }
        Rule::alter_drop_not_null => {
            let action_inner = action_pair.into_inner();
            let mut col_name = String::new();
            for p in action_inner {
                if p.as_rule() == Rule::identifier {
                    col_name = p.as_str().trim().to_string();
                }
            }
            AlterAction::DropNotNull { column: col_name }
        }
        Rule::alter_add_primary_key => {
            let action_inner = action_pair.into_inner();
            let columns: Vec<String> = action_inner
                .filter(|p| p.as_rule() == Rule::identifier)
                .map(|p| p.as_str().trim().to_string())
                .collect();
            AlterAction::AddPrimaryKey { columns }
        }
        Rule::alter_drop_primary_key => AlterAction::DropPrimaryKey,
        Rule::alter_add_foreign_key => {
            let sql = action_pair.as_str();
            let columns = if let Some(start) = sql.find("FOREIGN KEY (") {
                let inner = &sql[start + 13..];
                if let Some(end) = inner.find(')') {
                    inner[..end]
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect()
                } else {
                    vec![]
                }
            } else {
                vec![]
            };
            let after_ref = sql.split("REFERENCES").nth(1).unwrap_or("").trim();
            let ref_table = after_ref
                .split('(')
                .next()
                .unwrap_or("")
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_string();
            let ref_columns = if let Some(start) = after_ref.find('(') {
                if let Some(end) = after_ref.find(')') {
                    after_ref[start + 1..end]
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect()
                } else {
                    vec![]
                }
            } else {
                vec![]
            };
            AlterAction::AddForeignKey {
                name: None,
                columns,
                ref_table,
                ref_columns,
            }
        }
        Rule::alter_drop_foreign_key => {
            let mut action_inner = action_pair.into_inner();
            let name = action_inner
                .find(|p| p.as_rule() == Rule::identifier)
                .map(|p| p.as_str().trim().to_string())
                .unwrap_or_default();
            AlterAction::DropForeignKey { name }
        }
        Rule::alter_engine => {
            let engine = action_pair
                .into_inner()
                .next()
                .map(|p| p.as_str().trim().to_string())
                .unwrap_or_default();
            AlterAction::AlterEngine { engine }
        }
        Rule::alter_charset => {
            let mut charset = String::new();
            let mut collation = None;
            for p in action_pair.into_inner() {
                if p.as_rule() == Rule::identifier {
                    if charset.is_empty() {
                        charset = p.as_str().trim().to_string();
                    } else {
                        collation = Some(p.as_str().trim().to_string());
                    }
                }
            }
            AlterAction::AlterCharset { charset, collation }
        }
        _ => return Err(SqlError::Parse("Unknown ALTER TABLE action".to_string())),
    };

    Ok(SqlStmt::AlterTable(AlterTableStmt { table, action }))
}

fn parse_data_type(pair: pest::iterators::Pair<Rule>) -> SqlResult<DataType> {
    let type_str = pair.as_str().to_uppercase();
    match type_str.as_str() {
        "INT" | "INTEGER" => Ok(DataType::Int),
        "FLOAT" | "DOUBLE" | "REAL" => Ok(DataType::Float),
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" => Ok(DataType::Text),
        "BOOLEAN" | "BOOL" => Ok(DataType::Bool),
        "DATE" => Ok(DataType::Date),
        "DATETIME" | "TIMESTAMP" => Ok(DataType::DateTime),
        "BLOB" | "BINARY" => Ok(DataType::Blob),
        _ => Err(SqlError::Parse(format!("Unknown data type: {}", type_str))),
    }
}
