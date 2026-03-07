use pest::Parser;
use pest_derive::Parser;

use crate::storage::{Column, DataType, Value};

use super::ast::{
    BinaryOp, ComparisonOp, Condition, CreateTableStmt, DeleteStmt, DropTableStmt, Expression,
    InsertStmt, LimitClause, LogicalOp, Order, OrderByItem, SelectStmt, SqlStmt, UpdateStmt,
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
        let col_name = expect_identifier(col_inner.find(|p| p.as_rule() == Rule::identifier), "column name")?;
        let type_str = col_inner
            .find(|p| p.as_rule() == Rule::data_type)
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
    let inner = pair.into_inner();
    let name = inner
        .filter(|p| p.as_rule() == Rule::table_name)
        .last()
        .map(|p| p.as_str().trim().to_string())
        .ok_or("Missing table name")?;
    Ok(SqlStmt::DropTable(DropTableStmt { name }))
}

fn parse_update(pair: pest::iterators::Pair<Rule>) -> Result<SqlStmt, String> {
    let inner = pair.into_inner();
    let table = inner
        .clone()
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
            let col = expect_identifier(set_inner.find(|p| p.as_rule() == Rule::identifier), "column name")?;
            let expr = parse_expression(set_inner.find(|p| p.as_rule() == Rule::expression).ok_or("Missing expression")?)?;
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

fn parse_delete(pair: pest::iterators::Pair<Rule>) -> Result<SqlStmt, String> {
    let inner = pair.into_inner();
    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or("Missing table name")?;

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

fn parse_select(pair: pest::iterators::Pair<Rule>) -> Result<SqlStmt, String> {
    let inner = pair.into_inner();

    let _distinct = inner.clone().find(|p| p.as_rule() == Rule::distinct).is_some();

    let select_columns_pair = inner.clone().find(|p| p.as_rule() == Rule::select_columns).ok_or("Missing SELECT columns")?;
    let columns = parse_select_columns(select_columns_pair)?;

    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or("Missing table name")?;

    let where_clause = if let Some(p) = inner.clone().find(|p| p.as_rule() == Rule::where_clause) {
        Some(parse_where_clause(p)?)
    } else {
        None
    };

    let order_by = if let Some(p) = inner.clone().find(|p| p.as_rule() == Rule::order_by_clause) {
        parse_order_by(p)?
    } else {
        Vec::new()
    };

    let limit = if let Some(p) = inner.clone().find(|p| p.as_rule() == Rule::limit_clause) {
        Some(parse_limit(p)?)
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
        order_by,
        limit,
    }))
}

fn parse_order_by(pair: pest::iterators::Pair<Rule>) -> Result<Vec<OrderByItem>, String> {
    let mut inner = pair.into_inner();
    let list = inner.find(|p| p.as_rule() == Rule::order_by_list).ok_or("Missing ORDER BY list")?;
    let mut items = Vec::new();
    for item in list.into_inner() {
        if item.as_rule() == Rule::order_by_item {
            let mut it_inner = item.into_inner();
            let expr = parse_expression(it_inner.find(|p| p.as_rule() == Rule::expression).ok_or("Missing ORDER BY expression")?)?;
            let order = if let Some(op) = it_inner.find(|p| matches!(p.as_rule(), Rule::KW_ASC | Rule::KW_DESC)) {
                if op.as_rule() == Rule::KW_DESC {
                    Order::Desc
                } else {
                    Order::Asc
                }
            } else {
                Order::Asc
            };
            items.push(OrderByItem { expr, order });
        }
    }
    Ok(items)
}

fn parse_limit(pair: pest::iterators::Pair<Rule>) -> Result<LimitClause, String> {
    let mut inner = pair.into_inner();
    let count: usize = inner
        .find(|p| p.as_rule() == Rule::limit_count)
        .ok_or("Missing LIMIT count")?
        .as_str()
        .parse()
        .map_err(|e| format!("Invalid LIMIT count: {}", e))?;

    let offset = if let Some(off_pair) = inner.find(|p| p.as_rule() == Rule::offset_clause) {
        let off: usize = off_pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::limit_count)
            .ok_or("Missing OFFSET count")?
            .as_str()
            .parse()
            .map_err(|e| format!("Invalid OFFSET count: {}", e))?;
        Some(off)
    } else {
        None
    };

    Ok(LimitClause { count, offset })
}

fn parse_where_clause(pair: pest::iterators::Pair<Rule>) -> Result<Condition, String> {
    let mut inner = pair.into_inner();
    let cond_pair = inner.find(|p| p.as_rule() == Rule::condition).ok_or("Missing condition in WHERE clause")?;
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
                    let right = parse_expression(inner.find(|p| p.as_rule() == Rule::expression).ok_or("Missing right expression")?)?;
                    Ok(Condition::Comparison(left, op, right))
                }
                Rule::KW_IS => {
                    let is_not;
                    if let Some(next) = inner.next() {
                        if next.as_rule() == Rule::KW_NOT {
                            is_not = true;
                            let _null = inner.next().ok_or("Expected NULL after IS NOT")?;
                        } else if next.as_rule() == Rule::KW_NULL {
                            is_not = false;
                        } else {
                            return Err(format!("Expected NULL or NOT NULL after IS, got {:?}", next.as_rule()));
                        }
                    } else {
                        return Err("Missing token after IS".to_string());
                    }
                    
                    if is_not {
                        Ok(Condition::IsNotNull(left))
                    } else {
                        Ok(Condition::IsNull(left))
                    }
                }
                _ => Err(format!("Unexpected rule in condition: {:?}", op_pair.as_rule())),
            }
        }
        Rule::condition => {
            // "(" condition ")" (logical_op condition)?
            let left = parse_condition(first)?;
            if let Some(op_pair) = inner.find(|p| p.as_rule() == Rule::logical_op) {
                let op = match op_pair.into_inner().next().ok_or("Empty logical operator")?.as_rule() {
                    Rule::KW_AND => LogicalOp::And,
                    Rule::KW_OR => LogicalOp::Or,
                    r => return Err(format!("Unsupported logical operator: {:?}", r)),
                };
                let right = parse_condition(inner.find(|p| p.as_rule() == Rule::condition).ok_or("Missing right condition")?)?;
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
            let name = expect_identifier(cr_inner.find(|p| p.as_rule() == Rule::identifier), "column name")?;
            Ok(Expression::Column(name))
        }
        Rule::expression => parse_expression(first),
        Rule::KW_NOT => {
            // This is actually Condition::Not in our AST, but factor grammar has KW_NOT factor
            Err("NOT in expression factor not yet implemented".to_string())
        }
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
                let ce_inner = col_expr.into_inner();
                // Find expression inside column_expr
                let expr = ce_inner.filter(|p| p.as_rule() == Rule::expression).next().ok_or("Empty column expression")?;
                cols.push(expr.as_str().trim().to_string());
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
    let inner = pair.into_inner();

    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or("Missing table name")?;

    let value_list = inner
        .clone()
        .find(|p| p.as_rule() == Rule::value_list)
        .ok_or("Missing value list")?;
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
    let mut inner = pair.clone().into_inner();
    let p = match inner.next() {
        Some(p) => p,
        None => {
            let s = pair.as_str().trim();
            if s.to_uppercase() == "NULL" {
                return Ok(Value::Null);
            }
            return Err(format!("Empty literal: {}", s));
        }
    };

    match p.as_rule() {
        Rule::string_literal => Ok(Value::Text(p.as_str().trim_matches('\'').to_string())),
        Rule::number_literal => {
            let s = p.as_str().trim();
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
        Rule::boolean_literal => {
            let kw = p.into_inner().next().ok_or("Empty boolean literal")?;
            Ok(Value::Bool(kw.as_rule() == Rule::KW_TRUE))
        }
        Rule::KW_NULL => Ok(Value::Null),
        _ => Err(format!("Unknown literal rule: {:?}", p.as_rule())),
    }
}


fn expect_identifier(
    pair: Option<pest::iterators::Pair<Rule>>,
    ctx: &str,
) -> Result<String, String> {
    let p = pair.ok_or_else(|| format!("Missing {}", ctx))?;
    Ok(p.as_str().trim().to_string())
}
