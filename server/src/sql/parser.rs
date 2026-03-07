use pest::Parser;
use pest_derive::Parser;

use crate::storage::{Column, DataType, Value};

use super::ast::{
    AggregateType, BinaryOp, ComparisonOp, Condition, CreateTableStmt, DeleteStmt, DropTableStmt,
    Expression, FunctionCall, InsertStmt, LimitClause, LogicalOp, Order, OrderByItem, SelectColumn,
    SelectStmt, SqlStmt, UpdateStmt,
};
use super::error::{SqlError, SqlResult};

#[derive(Parser)]
#[grammar = "sql.pest"]
pub struct SqlParser;

pub fn parse(input: &str) -> SqlResult<SqlStmt> {
    let mut pairs = SqlParser::parse(Rule::statement, input.trim()).map_err(|e| SqlError::Parse(e.to_string()))?;

    let stmt_pair = pairs
        .next()
        .ok_or_else(|| SqlError::Parse("Empty SQL statement".to_string()))?;

    let mut inner = stmt_pair.into_inner();
    let kind_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Unable to determine statement type".to_string()))?;

    match kind_pair.as_rule() {
        Rule::select_stmt => parse_select(kind_pair),
        Rule::insert_stmt => parse_insert(kind_pair),
        Rule::create_table_stmt => parse_create_table(kind_pair),
        Rule::drop_table_stmt => parse_drop_table(kind_pair),
        Rule::update_stmt => parse_update(kind_pair),
        Rule::delete_stmt => parse_delete(kind_pair),
        _ => Err(SqlError::Parse("Unsupported SQL statement".to_string())),
    }
}

fn parse_create_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
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

fn parse_drop_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();
    let name = inner
        .filter(|p| p.as_rule() == Rule::table_name)
        .last()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
    Ok(SqlStmt::DropTable(DropTableStmt { name }))
}

fn parse_update(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();
    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

    let set_list = inner
        .clone()
        .find(|p| p.as_rule() == Rule::set_list)
        .ok_or_else(|| SqlError::Parse("Missing SET list".to_string()))?;
    let mut assignments = Vec::new();
    for item in set_list.into_inner() {
        if item.as_rule() == Rule::set_item {
            let mut set_inner = item.into_inner();
            let col = expect_identifier(set_inner.find(|p| p.as_rule() == Rule::identifier), "column name")?;
            let expr = parse_expression(set_inner.find(|p| p.as_rule() == Rule::expression).ok_or_else(|| SqlError::Parse("Missing expression".to_string()))?)?;
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

fn parse_delete(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();
    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

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

fn parse_select(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();

    let _distinct = inner.clone().find(|p| p.as_rule() == Rule::distinct).is_some();

    let select_columns_pair = inner.clone().find(|p| p.as_rule() == Rule::select_columns).ok_or_else(|| SqlError::Parse("Missing SELECT columns".to_string()))?;
    let columns = parse_select_columns(select_columns_pair)?;

    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

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
        columns,
        table,
        where_clause,
        order_by,
        limit,
    }))
}

fn parse_order_by(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<OrderByItem>> {
    let mut inner = pair.into_inner();
    let list = inner.find(|p| p.as_rule() == Rule::order_by_list).ok_or_else(|| SqlError::Parse("Missing ORDER BY list".to_string()))?;
    let mut items = Vec::new();
    for item in list.into_inner() {
        if item.as_rule() == Rule::order_by_item {
            let mut it_inner = item.into_inner();
            let expr = parse_expression(it_inner.find(|p| p.as_rule() == Rule::expression).ok_or_else(|| SqlError::Parse("Missing ORDER BY expression".to_string()))?)?;
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

fn parse_limit(pair: pest::iterators::Pair<Rule>) -> SqlResult<LimitClause> {
    let mut inner = pair.into_inner();
    let count: usize = inner
        .find(|p| p.as_rule() == Rule::limit_count)
        .ok_or_else(|| SqlError::Parse("Missing LIMIT count".to_string()))?
        .as_str()
        .parse()
        .map_err(|e| SqlError::Parse(format!("Invalid LIMIT count: {}", e)))?;

    let offset = if let Some(off_pair) = inner.find(|p| p.as_rule() == Rule::offset_clause) {
        let off: usize = off_pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::limit_count)
            .ok_or_else(|| SqlError::Parse("Missing OFFSET count".to_string()))?
            .as_str()
            .parse()
            .map_err(|e| SqlError::Parse(format!("Invalid OFFSET count: {}", e)))?;
        Some(off)
    } else {
        None
    };

    Ok(LimitClause { count, offset })
}

fn parse_where_clause(pair: pest::iterators::Pair<Rule>) -> SqlResult<Condition> {
    let mut inner = pair.into_inner();
    let cond_pair = inner.find(|p| p.as_rule() == Rule::condition).ok_or_else(|| SqlError::Parse("Missing condition in WHERE clause".to_string()))?;
    parse_condition(cond_pair)
}

fn parse_condition(pair: pest::iterators::Pair<Rule>) -> SqlResult<Condition> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or_else(|| SqlError::Parse("Empty condition".to_string()))?;

    match first.as_rule() {
        Rule::expression => {
            let left = parse_expression(first)?;
            let op_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing operator in condition".to_string()))?;
            
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
                        _ => return Err(SqlError::Parse(format!("Unsupported comparison operator: {}", op_pair.as_str()))),
                    };
                    let right = parse_expression(inner.find(|p| p.as_rule() == Rule::expression).ok_or_else(|| SqlError::Parse("Missing right expression".to_string()))?)?;
                    Ok(Condition::Comparison(left, op, right))
                }
                Rule::KW_IS => {
                    let is_not;
                    if let Some(next) = inner.next() {
                        if next.as_rule() == Rule::KW_NOT {
                            is_not = true;
                            let _null = inner.next().ok_or_else(|| SqlError::Parse("Expected NULL after IS NOT".to_string()))?;
                        } else if next.as_rule() == Rule::KW_NULL {
                            is_not = false;
                        } else {
                            return Err(SqlError::Parse(format!("Expected NULL or NOT NULL after IS, got {:?}", next.as_rule())));
                        }
                    } else {
                        return Err(SqlError::Parse("Missing token after IS".to_string()));
                    }
                    
                    if is_not {
                        Ok(Condition::IsNotNull(left))
                    } else {
                        Ok(Condition::IsNull(left))
                    }
                }
                _ => Err(SqlError::Parse(format!("Unexpected rule in condition: {:?}", op_pair.as_rule()))),
            }
        }
        Rule::condition => {
            // "(" condition ")" (logical_op condition)?
            let left = parse_condition(first)?;
            if let Some(op_pair) = inner.find(|p| p.as_rule() == Rule::logical_op) {
                let op = match op_pair.into_inner().next().ok_or_else(|| SqlError::Parse("Empty logical operator".to_string()))?.as_rule() {
                    Rule::KW_AND => LogicalOp::And,
                    Rule::KW_OR => LogicalOp::Or,
                    r => return Err(SqlError::Parse(format!("Unsupported logical operator: {:?}", r))),
                };
                let right = parse_condition(inner.find(|p| p.as_rule() == Rule::condition).ok_or_else(|| SqlError::Parse("Missing right condition".to_string()))?)?;
                Ok(Condition::Logical(Box::new(left), op, Box::new(right)))
            } else {
                Ok(left)
            }
        }
        _ => Err(SqlError::Parse(format!("Unsupported condition rule: {:?}", first.as_rule()))),
    }
}

fn parse_expression(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or_else(|| SqlError::Parse("Empty expression".to_string()))?;
    
    let mut left = parse_term(first)?;
    
    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "+" => BinaryOp::Add,
            "-" => BinaryOp::Sub,
            _ => return Err(SqlError::Parse(format!("Unsupported binary operator: {}", op_pair.as_str()))),
        };
        let right = parse_term(inner.next().ok_or_else(|| SqlError::Parse("Missing right term".to_string()))?)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }
    
    Ok(left)
}

fn parse_term(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or_else(|| SqlError::Parse("Empty term".to_string()))?;
    
    let mut left = parse_factor(first)?;
    
    while let Some(op_pair) = inner.next() {
        let op = match op_pair.as_str() {
            "*" => BinaryOp::Mul,
            "/" => BinaryOp::Div,
            _ => return Err(SqlError::Parse(format!("Unsupported binary operator: {}", op_pair.as_str()))),
        };
        let right = parse_factor(inner.next().ok_or_else(|| SqlError::Parse("Missing right factor".to_string()))?)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }
    
    Ok(left)
}

fn parse_factor(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let first = pair.into_inner().next().ok_or_else(|| SqlError::Parse("Empty factor".to_string()))?;
    
    match first.as_rule() {
        Rule::aggregate_func => parse_aggregate(first),
        Rule::literal => Ok(Expression::Literal(parse_literal(first)?)),
        Rule::column_ref => {
            let mut cr_inner = first.into_inner();
            let name = expect_identifier(cr_inner.find(|p| p.as_rule() == Rule::identifier), "column name")?;
            Ok(Expression::Column(name))
        }
        Rule::expression => parse_expression(first),
        Rule::KW_NOT => {
            // This is actually Condition::Not in our AST, but factor grammar has KW_NOT factor
            Err(SqlError::Parse("NOT in expression factor not yet implemented".to_string()))
        }
        _ => Err(SqlError::Parse(format!("Unsupported factor rule: {:?}", first.as_rule()))),
    }
}

fn parse_aggregate(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let agg_type_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing aggregate type".to_string()))?;
    let agg_type = parse_aggregate_type(agg_type_pair)?;
    
    let mut args = Vec::new();
    let arg_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing aggregate argument".to_string()))?;
    match arg_pair.as_rule() {
        Rule::star => args.push(Expression::Star),
        Rule::expression => args.push(parse_expression(arg_pair)?),
        _ => return Err(SqlError::Parse(format!("Unexpected aggregate argument: {:?}", arg_pair.as_rule()))),
    }
    
    Ok(Expression::FunctionCall(FunctionCall { name: agg_type, args }))
}

fn parse_aggregate_type(pair: pest::iterators::Pair<Rule>) -> SqlResult<AggregateType> {
    let kw = pair.into_inner().next().ok_or_else(|| SqlError::Parse("Missing aggregate keyword".to_string()))?;
    match kw.as_rule() {
        Rule::KW_COUNT => Ok(AggregateType::Count),
        Rule::KW_SUM => Ok(AggregateType::Sum),
        Rule::KW_AVG => Ok(AggregateType::Avg),
        Rule::KW_MIN => Ok(AggregateType::Min),
        Rule::KW_MAX => Ok(AggregateType::Max),
        _ => Err(SqlError::Parse(format!("Unknown aggregate type: {:?}", kw.as_rule()))),
    }
}

fn parse_select_columns(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<SelectColumn>> {
    let mut inner = pair.clone().into_inner();
    let first = match inner.next() {
        Some(p) => p,
        None => {
            if pair.as_str().trim() == "*" {
                return Ok(vec![SelectColumn { expr: Expression::Star, alias: None }]);
            }
            return Err(SqlError::Parse("Empty select columns".to_string()));
        }
    };

    if first.as_rule() == Rule::column_list {
        let mut cols = Vec::new();
        for col_expr in first.into_inner() {
            if col_expr.as_rule() == Rule::column_expr {
                let mut ce_inner = col_expr.into_inner();
                let expr = parse_expression(ce_inner.find(|p| p.as_rule() == Rule::expression).ok_or_else(|| SqlError::Parse("Empty column expression".to_string()))?)?;
                let alias = ce_inner.find(|p| p.as_rule() == Rule::alias).and_then(|p| {
                    p.into_inner().find(|p2| p2.as_rule() == Rule::identifier).map(|p3| p3.as_str().to_string())
                });
                cols.push(SelectColumn { expr, alias });
            }
        }
        Ok(cols)
    } else if first.as_str().trim() == "*" {
        Ok(vec![SelectColumn { expr: Expression::Star, alias: None }])
    } else {
        // Fallback for single column if not in list
        let expr = parse_expression(first)?;
        Ok(vec![SelectColumn { expr, alias: None }])
    }
}

fn parse_insert(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();

    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

    let value_list = inner
        .clone()
        .find(|p| p.as_rule() == Rule::value_list)
        .ok_or_else(|| SqlError::Parse("Missing value list".to_string()))?;
    let values = parse_value_list(value_list)?;

    Ok(SqlStmt::Insert(InsertStmt { table, values }))
}

fn parse_value_list(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<Value>> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::literal)
        .map(parse_literal)
        .collect()
}

fn parse_literal(pair: pest::iterators::Pair<Rule>) -> SqlResult<Value> {
    let mut inner = pair.clone().into_inner();
    let p = match inner.next() {
        Some(p) => p,
        None => {
            let s = pair.as_str().trim();
            if s.to_uppercase() == "NULL" {
                return Ok(Value::Null);
            }
            return Err(SqlError::Parse(format!("Empty literal: {}", s)));
        }
    };

    match p.as_rule() {
        Rule::string_literal => Ok(Value::Text(p.as_str().trim_matches('\'').to_string())),
        Rule::number_literal => {
            let s = p.as_str().trim();
            if s.contains('.') {
                s.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| SqlError::Parse(format!("Invalid number: {}", s)))
            } else {
                s.parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| SqlError::Parse(format!("Invalid integer: {}", s)))
            }
        }
        Rule::boolean_literal => {
            let kw = p.into_inner().next().ok_or_else(|| SqlError::Parse("Empty boolean literal".to_string()))?;
            Ok(Value::Bool(kw.as_rule() == Rule::KW_TRUE))
        }
        Rule::KW_NULL => Ok(Value::Null),
        _ => Err(SqlError::Parse(format!("Unknown literal rule: {:?}", p.as_rule()))),
    }
}


fn expect_identifier(
    pair: Option<pest::iterators::Pair<Rule>>,
    ctx: &str,
) -> SqlResult<String> {
    let p = pair.ok_or_else(|| SqlError::Parse(format!("Missing {}", ctx)))?;
    Ok(p.as_str().trim().to_string())
}
