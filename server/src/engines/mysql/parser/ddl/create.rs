use super::super::utils::expect_identifier;
use crate::engines::mysql::ast::{
    AlterViewStmt, CallStmt, CreateDatabaseStmt, CreateIndexStmt, CreateMaterializedViewStmt,
    CreateProcedureStmt, CreateTableStmt, CreateTriggerStmt, CreateViewStmt, DropProcedureStmt,
    DropViewStmt, IndexType, SqlStmt, TriggerEvent, TriggerTiming,
};
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::engines::mysql::parser::Rule;
use crate::storage::{Column, DataType, ForeignKey};

pub fn parse_create_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let name = inner
        .find(|p: &pest::iterators::Pair<Rule>| p.as_rule() == Rule::table_name)
        .map(|p: pest::iterators::Pair<Rule>| {
            let column_ref_rule = p.into_inner().next().unwrap();
            column_ref_rule
                .into_inner()
                .filter(|pi| pi.as_rule() == Rule::path_identifier)
                .map(|pi| pi.as_str().trim().to_string())
                .collect::<Vec<_>>()
                .join(".")
        })
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

    let create_definitions_pair = inner
        .find(|p: &pest::iterators::Pair<Rule>| p.as_rule() == Rule::create_definitions)
        .ok_or_else(|| SqlError::Parse("Missing table definitions".to_string()))?;

    let mut columns = Vec::new();
    let mut primary_key = None;
    let mut foreign_keys = Vec::new();

    for def_wrapper in create_definitions_pair.into_inner() {
        if def_wrapper.as_rule() != Rule::create_definition {
            continue;
        }

        let def = def_wrapper.into_inner().next().unwrap();
        match def.as_rule() {
            Rule::column_def => {
                let mut col_inner = def.clone().into_inner();
                let col_name = expect_identifier(col_inner.next(), "column name")?;

                let col = parse_column_def(def)?;

                // Check if this column has PRIMARY KEY attribute
                let has_pk = {
                    let mut pk = false;
                    for attr in col_inner.skip(1) {
                        // Skip type
                        if attr.as_rule() == Rule::column_attribute
                            && attr.as_str().to_uppercase().contains("PRIMARY")
                        {
                            pk = true;
                            break;
                        }
                    }
                    pk
                };

                if has_pk {
                    primary_key.get_or_insert_with(Vec::new).push(col_name);
                }

                columns.push(col);
            }
            Rule::primary_key_def => {
                let mut pk_inner = def.into_inner();
                let _ = pk_inner.next(); // KW_PRIMARY
                let _ = pk_inner.next(); // KW_KEY
                let id_list = pk_inner.next().unwrap();
                let cols: Vec<String> = id_list
                    .into_inner()
                    .map(|p| p.as_str().trim().to_string())
                    .collect();
                primary_key = Some(cols);
            }
            Rule::foreign_key_def => {
                let mut fk_inner = def.into_inner();
                let _ = fk_inner.next(); // KW_FOREIGN
                let _ = fk_inner.next(); // KW_KEY

                let local_cols: Vec<String> = fk_inner
                    .next()
                    .unwrap()
                    .into_inner()
                    .map(|p| p.as_str().trim().to_string())
                    .collect();

                let _ = fk_inner.next(); // KW_REFERENCES
                let ref_table = fk_inner.next().unwrap().as_str().trim().to_string();
                let ref_cols: Vec<String> = fk_inner
                    .next()
                    .unwrap()
                    .into_inner()
                    .map(|p| p.as_str().trim().to_string())
                    .collect();

                foreign_keys.push(ForeignKey {
                    name: String::new(),
                    columns: local_cols,
                    ref_table,
                    ref_columns: ref_cols,
                });
            }
            _ => {}
        }
    }

    Ok(SqlStmt::CreateTable(CreateTableStmt {
        name,
        columns,
        primary_key,
        foreign_keys,
    }))
}

pub fn parse_create_materialized_view(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_CREATE, KW_MATERIALIZED, KW_VIEW
    let _ = inner.next();
    let _ = inner.next();
    let _ = inner.next();

    let name = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing view name".to_string()))?;

    // Skip KW_AS
    let _ = inner.next();

    let select_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing SELECT in CREATE MATERIALIZED VIEW".to_string()))?;

    let query = super::super::select::parse_select_inner(select_pair)?;

    Ok(SqlStmt::CreateMaterializedView(
        CreateMaterializedViewStmt { name, query },
    ))
}

pub fn parse_create_view(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_CREATE, KW_VIEW
    let _ = inner.next();
    let _ = inner.next();

    let name = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing view name".to_string()))?;

    let mut columns = None;
    let mut query = None;
    let mut with_check_option = false;

    for p in inner {
        match p.as_rule() {
            Rule::column_list => {
                columns = Some(
                    p.into_inner()
                        .map(|c| c.as_str().trim().to_string())
                        .collect(),
                );
            }
            Rule::select_stmt_inner => {
                query = Some(super::super::select::parse_select_inner(p)?);
            }
            Rule::check_option => {
                with_check_option = true;
            }
            _ => {}
        }
    }

    let query =
        query.ok_or_else(|| SqlError::Parse("Missing SELECT in CREATE VIEW".to_string()))?;

    Ok(SqlStmt::CreateView(CreateViewStmt {
        name,
        query,
        columns,
        with_check_option,
    }))
}

pub fn parse_drop_view(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_DROP, KW_VIEW
    let _ = inner.next();
    let _ = inner.next();

    let name = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing view name".to_string()))?;

    Ok(SqlStmt::DropView(DropViewStmt { name }))
}

pub fn parse_alter_view(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_ALTER, KW_VIEW
    let _ = inner.next();
    let _ = inner.next();

    let name = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing view name".to_string()))?;

    // Skip KW_AS
    let _ = inner.next();

    let select_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing SELECT in ALTER VIEW".to_string()))?;

    let query = super::super::select::parse_select_inner(select_pair)?;

    Ok(SqlStmt::AlterView(AlterViewStmt { name, query }))
}

pub fn parse_create_procedure(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_CREATE, KW_PROCEDURE
    let _ = inner.next();
    let _ = inner.next();

    let name = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing procedure name".to_string()))?;

    // Skip KW_AS
    let _ = inner.next();

    let stmt_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing procedure body".to_string()))?;

    let body = super::super::parse(stmt_pair.as_str())?.into();

    Ok(SqlStmt::CreateProcedure(CreateProcedureStmt { name, body }))
}

pub fn parse_drop_procedure(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_DROP, KW_PROCEDURE
    let _ = inner.next();
    let _ = inner.next();

    let name = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing procedure name".to_string()))?;

    Ok(SqlStmt::DropProcedure(DropProcedureStmt { name }))
}

pub fn parse_column_def(pair: pest::iterators::Pair<Rule>) -> SqlResult<Column> {
    let mut col_inner = pair.into_inner();
    let col_name = expect_identifier(
        col_inner.next(), // identifier is first
        "column name",
    )?;
    let type_pair = col_inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing column type".to_string()))?;

    let type_str = type_pair.as_str().to_uppercase();

    let mut is_auto_increment = false;
    if type_str == "SERIAL" {
        is_auto_increment = true;
    }

    // Parse attributes
    for attr in col_inner {
        if attr.as_rule() == Rule::column_attribute {
            let attr_str = attr.as_str().to_uppercase();
            if attr_str == "AUTO_INCREMENT" {
                is_auto_increment = true;
            }
        }
    }

    let data_type = if type_str == "SERIAL" {
        DataType::Int
    } else if type_pair.as_rule() == Rule::parameterized_type {
        parse_parameterized_type(type_pair)
    } else {
        DataType::from_str(&type_str)
    };

    Ok(Column {
        name: col_name,
        data_type,
        is_auto_increment,
        is_not_null: false,
        default_value: None,
    })
}

fn parse_parameterized_type(pair: pest::iterators::Pair<Rule>) -> DataType {
    let inner = pair.into_inner();
    let type_name = inner
        .clone()
        .next()
        .map(|p| p.as_str().to_uppercase())
        .unwrap_or_default();

    let values: Vec<String> = inner
        .skip(1) // Skip type name
        .flat_map(|p| {
            if p.as_rule() == Rule::param_value {
                vec![p.as_str().trim_matches('\'').to_uppercase()]
            } else {
                vec![]
            }
        })
        .collect();

    match type_name.as_str() {
        "DECIMAL" | "NUMERIC" => {
            if values.len() >= 2 {
                let p: usize = values[0].parse().unwrap_or(10);
                let s: usize = values[1].parse().unwrap_or(0);
                DataType::Decimal(p, s)
            } else {
                DataType::Decimal(10, 0)
            }
        }
        "CHAR" | "CHARACTER" => {
            let len: usize = values.first().and_then(|v| v.parse().ok()).unwrap_or(1);
            DataType::Char(len)
        }
        "VARCHAR" | "CHARACTER VARYING" => {
            let len: usize = values.first().and_then(|v| v.parse().ok()).unwrap_or(255);
            DataType::VarChar(Some(len))
        }
        "BINARY" => {
            let len: usize = values.first().and_then(|v| v.parse().ok()).unwrap_or(1);
            DataType::Binary(Some(len))
        }
        "VARBINARY" => {
            let len: usize = values.first().and_then(|v| v.parse().ok()).unwrap_or(255);
            DataType::VarBinary(Some(len))
        }
        "ENUM" => DataType::Enum(values),
        "SET" => DataType::Set(values),
        _ => DataType::Text,
    }
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
            Rule::table_name => {
                let column_ref_rule = p.into_inner().next().unwrap();
                table = Some(
                    column_ref_rule
                        .into_inner()
                        .filter(|pi| pi.as_rule() == Rule::path_identifier)
                        .map(|pi| pi.as_str().trim().to_string())
                        .collect::<Vec<_>>()
                        .join("."),
                )
            }
            Rule::index_expression_list => {
                for expr_pair in p.into_inner() {
                    if expr_pair.as_rule() == Rule::expression {
                        expressions.push(super::super::expr::parse_expression(expr_pair)?);
                    }
                }
            }
            Rule::index_type_clause => {
                let type_inner = p
                    .into_inner()
                    .find(|it| it.as_rule() == Rule::index_type)
                    .ok_or_else(|| SqlError::Parse("Missing index type".to_string()))?;
                if type_inner.as_str().to_uppercase() == "HASH" {
                    index_type = IndexType::Hash;
                }
            }
            Rule::where_clause => {
                where_clause = Some(super::super::expr::parse_where_clause(p)?);
            }
            _ => {}
        }
    }

    let name = index_name.ok_or_else(|| SqlError::Parse("Missing index name".to_string()))?;
    let table = table.ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
    if expressions.is_empty() {
        return Err(SqlError::Parse(
            "Index must have at least one expression".to_string(),
        ));
    }

    Ok(SqlStmt::CreateIndex(CreateIndexStmt {
        name,
        table,
        expressions,
        unique,
        index_type,
        where_clause,
    }))
}

pub fn parse_create_database(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let name = inner
        .find(|p| p.as_rule() == Rule::identifier)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing database name".to_string()))?;

    let if_not_exists = inner.find(|p| p.as_rule() == Rule::if_not_exists).is_some();

    Ok(SqlStmt::CreateDatabase(CreateDatabaseStmt {
        name,
        if_not_exists,
    }))
}

pub fn parse_create_trigger(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let pair_str = pair.as_str().to_uppercase();
    let mut inner = pair.into_inner();

    let name = inner
        .find(|p| p.as_rule() == Rule::identifier)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing trigger name".to_string()))?;

    let timing = if pair_str.contains("BEFORE") {
        TriggerTiming::Before
    } else {
        TriggerTiming::After
    };

    let event = if pair_str.contains("INSERT") {
        TriggerEvent::Insert
    } else if pair_str.contains("UPDATE") {
        TriggerEvent::Update
    } else {
        TriggerEvent::Delete
    };

    let table = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| {
            p.into_inner()
                .filter(|pi| pi.as_rule() == Rule::path_identifier)
                .map(|pi| pi.as_str().trim().to_string())
                .collect::<Vec<_>>()
                .join(".")
        })
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

    let body = inner
        .find(|p| p.as_rule() == Rule::trigger_body)
        .map(|p| p.as_str().trim().to_string())
        .unwrap_or_default();

    Ok(SqlStmt::CreateTrigger(CreateTriggerStmt {
        name,
        timing,
        event,
        table,
        body,
    }))
}
