use pest::Parser;
use pest_derive::Parser;

use crate::storage::{Column, DataType, Value};

use super::ast::{
    BinaryOp, ComparisonOp, Condition, CreateTableStmt, DeleteStmt, DropTableStmt, Expression,
    InsertStmt, LogicalOp, SelectStmt, SqlStmt, UpdateStmt,
};

#[derive(Parser)]
#[grammar = "sql.pest"]
pub struct SqlParser;

pub fn parse(input: &str) -> Result<SqlStmt, String> {
    let mut pairs = SqlParser::parse(Rule::statement, input.trim()).map_err(|e| e.to_string())?;

    let stmt_pair = pairs
        .next()
        .ok_or_else(|| "Empty SQL statement".to_string())?;

    let mut inner = stmt_pair.into_inner();
    let kind_pair = inner
        .next()
        .ok_or_else(|| "Unable to determine statement type".to_string())?;

    match kind_pair.as_rule() {
        Rule::select_stmt => parse_select(kind_pair),
        Rule::insert_stmt => parse_insert(kind_pair),
        Rule::create_table_stmt => parse_create_table(kind_pair),
        Rule::drop_table_stmt => parse_drop_table(kind_pair),
        Rule::update_stmt => parse_update(kind_pair),
        Rule::delete_stmt => parse_delete(kind_pair),
        _ => Err("Unsupported SQL statement".to_string()),
    }
}

fn parse_create_table(pair: pest::iterators::Pair<Rule>) -> Result<SqlStmt, String> {
    let mut inner = pair.into_inner();
    let name = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or("Missing table name")?;
    let column_defs = inner
        .find(|p| p.as_rule() == Rule::column_defs)
        .ok_or("Missing column definitions")?
        .into_inner();

    let mut columns = Vec::new();
    for col_def in column_defs {
        if col_def.as_rule() != Rule::column_def {
            continue;
        }
        let mut col_inner = col_def.into_inner();
        let col_name = expect_identifier(col_inner.next(), "column name")?;
        let type_str = col_inner
            .next()
            .ok_or("Missing column type")?
            .as_str()
            .to_uppercase();
        columns.push(Column {
            name: col_name,
            data_type: DataType::from_str(&type_str),
        });
    }

    Ok(SqlStmt::CreateTable(CreateTableStmt { name, columns }))
}

fn parse_drop_table(pair: pest::iterators::Pair<Rule>) -> Result<SqlStmt, String> {
    let mut inner = pair.into_inner();
    let name = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or("Missing table name")?;
    Ok(SqlStmt::DropTable(DropTableStmt { name }))
}

fn parse_update(pair: pest::iterators::Pair<Rule>) -> Result<SqlStmt, String> {
    let mut inner = pair.into_inner();
    let table = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or("Missing table name")?;

    let set_list = inner
        .clone()
        .find(|p| p.as_rule() == Rule::set_list)
        .ok_or("Missing SET list")?;
    let mut assignments = Vec::new();
    for item in set_list.into_inner() {
        if item.as_rule() == Rule::set_item {
            let mut set_inner = item.into_inner();
            let col = expect_identifier(set_inner.next(), "column name")?;
            let expr = parse_expression(set_inner.next().ok_or("Missing expression")?)?;
            assignments.push((col, expr));
        }
    }

    let where_clause = if let Some(p) = inner.find(|p| p.as_rule() == Rule::where_clause) {
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

fn parse_delete(pair: pest::iterators::Pair<Rule>) -> Result<SqlStmt, String> {
    let mut inner = pair.into_inner();
    let table = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or("Missing table name")?;

    let where_clause = if let Some(p) = inner.find(|p| p.as_rule() == Rule::where_clause) {
        Some(parse_where_clause(p)?)
    } else {
        None
    };

    Ok(SqlStmt::Delete(DeleteStmt {
        table,
        where_clause,
    }))
}

fn parse_select(pair: pest::iterators::Pair<Rule>) -> Result<SqlStmt, String> {
    let mut inner = pair.into_inner();

    let mut peek = inner.next();
    if peek.as_ref().map(|p| p.as_rule()) == Some(Rule::distinct) {
        peek = inner.next();
    }

    let select_columns_pair = peek.ok_or("Missing SELECT columns")?;
    let columns = parse_select_columns(select_columns_pair)?;

    // Use clone and find to avoid consuming the iterator prematurely
    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or("Missing table name")?;

    let where_clause = if let Some(p) = inner.find(|p| p.as_rule() == Rule::where_clause) {
        Some(parse_where_clause(p)?)
    } else {
        None
    };

    Ok(SqlStmt::Select(SelectStmt {
        columns: if columns.is_empty() {
            vec!["*".to_string()]
        } else {
            columns
        },
        table,
        where_clause,
    }))
}

fn parse_where_clause(pair: pest::iterators::Pair<Rule>) -> Result<Condition, String> {
    let mut inner = pair.into_inner();
    let cond_pair = inner.next().ok_or("Missing condition in WHERE clause")?;
    parse_condition(cond_pair)
}

fn parse_condition(pair: pest::iterators::Pair<Rule>) -> Result<Condition, String> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or("Empty condition")?;

    match first.as_rule() {
        Rule::expression => {
            let left = parse_expression(first)?;
            let op_pair = inner.next().ok_or("Missing operator in condition")?;
            
            match op_pair.as_rule() {
                Rule::comparison_op => {
                    let op = match op_pair.as_str().to_uppercase().as_str() {
                        "=" => ComparisonOp::Eq,
                        "!=" | "<>" => ComparisonOp::NotEq,
                        "<" => ComparisonOp::Lt,
                        ">" => ComparisonOp::Gt,
                        "<=" => ComparisonOp::LtEq,
                        ">=" => ComparisonOp::GtEq,
                        "LIKE" => ComparisonOp::Like,
                        _ => return Err(format!("Unsupported comparison operator: {}", op_pair.as_str())),
                    };
                    let right = parse_expression(inner.next().ok_or("Missing right expression")?)?;
                    Ok(Condition::Comparison(left, op, right))
                }
                _ => {
                    if op_pair.as_str().to_uppercase() == "IS" {
                         let next = inner.next().ok_or("Expected NULL or NOT NULL after IS")?;
                         if next.as_str().to_uppercase() == "NOT" {
                             let _null = inner.next().ok_or("Expected NULL after IS NOT")?;
                             Ok(Condition::IsNotNull(left))
                         } else if next.as_str().to_uppercase() == "NULL" {
                             Ok(Condition::IsNull(left))
                         } else {
                             Err(format!("Unexpected token after IS: {}", next.as_str()))
                         }
                    } else {
                        Err(format!("Unexpected token in condition: {}", op_pair.as_str()))
                    }
                }
            }
        }
        Rule::condition => {
            // "(" condition ")" logical_op? condition?
            let left = parse_condition(first)?;
            if let Some(op_pair) = inner.next() {
                let op = match op_pair.as_rule() {
                    Rule::logical_op => match op_pair.as_str().to_uppercase().as_str() {
                        "AND" => LogicalOp::And,
                        "OR" => LogicalOp::Or,
                        _ => return Err(format!("Unsupported logical operator: {}", op_pair.as_str())),
                    },
                    _ => return Ok(left),
                };
                let right = parse_condition(inner.next().ok_or("Missing right condition")?)?;
                Ok(Condition::Logical(Box::new(left), op, Box::new(right)))
            } else {
                Ok(left)
            }
        }
        _ => Err(format!("Unsupported condition rule: {:?}", first.as_rule())),
    }
}

fn parse_expression(pair: pest::iterators::Pair<Rule>) -> Result<Expression, String> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or("Empty expression")?;
    
    // Simplistic expression parsing for now (term ((+ | -) term)*)
    let mut left = parse_term(first)?;
    
    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "+" => BinaryOp::Add,
            "-" => BinaryOp::Sub,
            _ => return Err(format!("Unsupported binary operator: {}", op_pair.as_str())),
        };
        let right = parse_term(inner.next().ok_or("Missing right term")?)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }
    
    Ok(left)
}

fn parse_term(pair: pest::iterators::Pair<Rule>) -> Result<Expression, String> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or("Empty term")?;
    
    let mut left = parse_factor(first)?;
    
    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "*" => BinaryOp::Mul,
            "/" => BinaryOp::Div,
            _ => return Err(format!("Unsupported binary operator: {}", op_pair.as_str())),
        };
        let right = parse_factor(inner.next().ok_or("Missing right factor")?)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }
    
    Ok(left)
}

fn parse_factor(pair: pest::iterators::Pair<Rule>) -> Result<Expression, String> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or("Empty factor")?;
    
    match first.as_rule() {
        Rule::literal => Ok(Expression::Literal(parse_literal(first)?)),
        Rule::column_ref => {
            let mut cr_inner = first.into_inner();
            let name = expect_identifier(cr_inner.next(), "column name")?;
            Ok(Expression::Column(name))
        }
        Rule::expression => parse_expression(first),
        _ => Err(format!("Unsupported factor rule: {:?}", first.as_rule())),
    }
}

fn parse_select_columns(pair: pest::iterators::Pair<Rule>) -> Result<Vec<String>, String> {
    let pair_str = pair.as_str().trim().to_string();
    let mut inner = pair.into_inner();
    let first = match inner.next() {
        Some(p) => p,
        None => {
            return if pair_str.contains('*') {
                Ok(vec!["*".to_string()])
            } else {
                Err("Empty select columns".to_string())
            };
        }
    };

    if first.as_rule() == Rule::column_list {
        let mut cols = Vec::new();
        for col_expr in first.into_inner() {
            if col_expr.as_rule() == Rule::column_expr {
                let expr_pair = col_expr
                    .into_inner()
                    .next()
                    .ok_or("Empty column expression")?;
                cols.push(expr_pair.as_str().trim().to_string());
            }
        }
        Ok(cols)
    } else if first.as_str().trim() == "*" {
        Ok(vec!["*".to_string()])
    } else {
        Ok(vec![first.as_str().trim().to_string()])
    }
}

fn parse_insert(pair: pest::iterators::Pair<Rule>) -> Result<SqlStmt, String> {
    let mut inner = pair.into_inner();

    // Skip INSERT, INTO - find table_name
    let table = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or("Missing table name")?;

    let mut value_list_pair = None;
    for p in inner {
        if p.as_rule() == Rule::value_list {
            value_list_pair = Some(p);
            break;
        }
    }

    let value_list = value_list_pair.ok_or("Missing value list")?;
    let values = parse_value_list(value_list)?;

    Ok(SqlStmt::Insert(InsertStmt { table, values }))
}

fn parse_value_list(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Value>, String> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::literal)
        .map(parse_literal)
        .collect()
}

fn parse_literal(pair: pest::iterators::Pair<Rule>) -> Result<Value, String> {
    let s = pair.as_str().trim();
    if s.to_uppercase() == "NULL" {
        return Ok(Value::Null);
    }

    let inner = pair.into_inner().next();
    let (rule, s) = match &inner {
        Some(p) => (p.as_rule(), p.as_str().trim()),
        None => return Err(format!("Unknown literal: {}", s)),
    };

    match rule {
        Rule::string_literal => Ok(Value::Text(s.trim_matches('\'').to_string())),
        Rule::number_literal => {
            if s.contains('.') {
                s.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| format!("Invalid number: {}", s))
            } else {
                s.parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| format!("Invalid integer: {}", s))
            }
        }
        Rule::boolean_literal => Ok(Value::Bool(s.to_lowercase() == "true")),
        _ => {
            if s.to_uppercase() == "NULL" {
                Ok(Value::Null)
            } else {
                Err(format!("Unknown literal: {}", s))
            }
        }
    }
}


fn expect_identifier(
    pair: Option<pest::iterators::Pair<Rule>>,
    ctx: &str,
) -> Result<String, String> {
    let p = pair.ok_or_else(|| format!("Missing {}", ctx))?;
    let rule = p.as_rule();
    let s = p.as_str().trim().to_string();
    if rule == Rule::identifier || rule == Rule::table_name {
        Ok(s)
    } else {
        Ok(s)
    }
}
