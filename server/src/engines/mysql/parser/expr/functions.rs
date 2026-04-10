use super::parse_any_expression;
use crate::engines::mysql::ast::{
    AggregateType, Expression, FrameBound, FrameUnits, FunctionCall, Order, ScalarFuncType,
    ScalarFunction, WindowFrame, WindowFuncType, WindowFunction, WindowOrderByItem,
};
use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::engines::mysql::parser::Rule;

pub fn parse_aggregate(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let agg_type_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing aggregate type".to_string()))?;
    let agg_type = parse_aggregate_type(agg_type_pair)?;

    let mut args = Vec::new();
    for arg_pair in inner {
        match arg_pair.as_rule() {
            Rule::star => args.push(Expression::Star),
            Rule::expression => args.push(parse_any_expression(arg_pair)?),
            _ => {
                if arg_pair.as_str() == "*" {
                    args.push(Expression::Star);
                } else {
                    return Err(SqlError::Parse(format!(
                        "Unexpected aggregate argument: {:?}",
                        arg_pair.as_rule()
                    )));
                }
            }
        }
    }

    if args.is_empty() {
        return Err(SqlError::Parse("Missing aggregate argument".to_string()));
    }

    Ok(Expression::FunctionCall(FunctionCall {
        name: agg_type,
        args,
    }))
}

pub fn parse_aggregate_type(pair: pest::iterators::Pair<Rule>) -> SqlResult<AggregateType> {
    let kw = pair
        .into_inner()
        .next()
        .ok_or_else(|| SqlError::Parse("Missing aggregate keyword".to_string()))?;
    match kw.as_rule() {
        Rule::KW_COUNT => Ok(AggregateType::Count),
        Rule::KW_SUM => Ok(AggregateType::Sum),
        Rule::KW_AVG => Ok(AggregateType::Avg),
        Rule::KW_MIN => Ok(AggregateType::Min),
        Rule::KW_MAX => Ok(AggregateType::Max),
        Rule::KW_GROUP_CONCAT => Ok(AggregateType::GroupConcat),
        Rule::KW_JSON_ARRAYAGG => Ok(AggregateType::JsonArrayAgg),
        Rule::KW_JSON_OBJECTAGG => Ok(AggregateType::JsonObjectAgg),
        _ => Err(SqlError::Parse(format!(
            "Unknown aggregate type: {:?}",
            kw.as_rule()
        ))),
    }
}

pub fn parse_scalar_func(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let name_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing scalar function name".to_string()))?;
    let name = parse_scalar_func_type(name_pair)?;

    let mut args = Vec::new();
    for arg_pair in inner {
        match arg_pair.as_rule() {
            Rule::KW_NULL => args.push(Expression::Literal(crate::storage::Value::Null)),
            Rule::expression
            | Rule::literal
            | Rule::string_literal
            | Rule::number_literal
            | Rule::boolean_literal
            | Rule::placeholder => {
                args.push(parse_any_expression(arg_pair)?);
            }
            _ => {}
        }
    }

    Ok(Expression::ScalarFunc(ScalarFunction { name, args }))
}

pub fn parse_scalar_func_type(pair: pest::iterators::Pair<Rule>) -> SqlResult<ScalarFuncType> {
    let name = pair.as_str().to_uppercase();
    match name.as_str() {
        "LOWER" => Ok(ScalarFuncType::Lower),
        "UPPER" => Ok(ScalarFuncType::Upper),
        "LENGTH" => Ok(ScalarFuncType::Length),
        "ABS" => Ok(ScalarFuncType::Abs),
        "NOW" => Ok(ScalarFuncType::Now),
        "CONCAT" => Ok(ScalarFuncType::Concat),
        "COALESCE" => Ok(ScalarFuncType::Coalesce),
        "REPLACE" => Ok(ScalarFuncType::Replace),
        "IFNULL" => Ok(ScalarFuncType::IfNull),
        "IF" => Ok(ScalarFuncType::If),
        "DATEDIFF" => Ok(ScalarFuncType::DateDiff),
        "DATE_FORMAT" => Ok(ScalarFuncType::DateFormat),
        "MD5" => Ok(ScalarFuncType::Md5),
        "SHA2" => Ok(ScalarFuncType::Sha2),
        _ => Err(SqlError::Parse(format!(
            "Unknown scalar function: {}",
            name
        ))),
    }
}

pub fn parse_window_function(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();

    let name_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing window function name".to_string()))?;
    let func_type = parse_window_func_type(name_pair)?;

    let mut args = Vec::new();
    for arg_pair in inner.by_ref() {
        match arg_pair.as_rule() {
            Rule::star => args.push(Expression::Star),
            Rule::expression => args.push(parse_any_expression(arg_pair)?),
            Rule::KW_OVER => break,
            _ => {}
        }
    }

    let window_spec = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing OVER clause".to_string()))?;

    let (partition_by, order_by, frame) = parse_window_spec(window_spec)?;

    Ok(Expression::WindowFunc(WindowFunction {
        func_type,
        args,
        partition_by,
        order_by,
        frame,
    }))
}

fn parse_window_func_type(pair: pest::iterators::Pair<Rule>) -> SqlResult<WindowFuncType> {
    let name = pair.as_str().to_uppercase();
    match name.as_str() {
        "ROW_NUMBER" => Ok(WindowFuncType::RowNumber),
        "RANK" => Ok(WindowFuncType::Rank),
        "DENSE_RANK" => Ok(WindowFuncType::DenseRank),
        "NTILE" => Ok(WindowFuncType::Ntile),
        "PERCENT_RANK" => Ok(WindowFuncType::PercentRank),
        "CUME_DIST" => Ok(WindowFuncType::CumeDist),
        "FIRST_VALUE" => Ok(WindowFuncType::FirstValue),
        "LAST_VALUE" => Ok(WindowFuncType::LastValue),
        "NTH_VALUE" => Ok(WindowFuncType::NthValue),
        "LAG" => Ok(WindowFuncType::Lag),
        "LEAD" => Ok(WindowFuncType::Lead),
        _ => Err(SqlError::Parse(format!(
            "Unknown window function: {}",
            name
        ))),
    }
}

#[allow(clippy::type_complexity)]
fn parse_window_spec(
    pair: pest::iterators::Pair<Rule>,
) -> SqlResult<(
    Vec<Expression>,
    Vec<WindowOrderByItem>,
    Option<Box<WindowFrame>>,
)> {
    let mut partition_by = Vec::new();
    let mut order_by = Vec::new();
    let mut frame = None;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::window_partition_by => {
                parse_window_partition_by(inner_pair, &mut partition_by)?;
            }
            Rule::window_order_by => {
                parse_window_order_by(inner_pair, &mut order_by)?;
            }
            Rule::window_frame => {
                frame = Some(parse_window_frame(inner_pair)?);
            }
            _ => {}
        }
    }

    Ok((partition_by, order_by, frame))
}

fn parse_window_partition_by(
    pair: pest::iterators::Pair<Rule>,
    partition_by: &mut Vec<Expression>,
) -> SqlResult<()> {
    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::expression => {
                partition_by.push(parse_any_expression(inner_pair)?);
            }
            Rule::expression_list => {
                for expr_pair in inner_pair.into_inner() {
                    if expr_pair.as_rule() == Rule::expression {
                        partition_by.push(parse_any_expression(expr_pair)?);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_window_order_by(
    pair: pest::iterators::Pair<Rule>,
    order_by: &mut Vec<WindowOrderByItem>,
) -> SqlResult<()> {
    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::order_by_item => {
                order_by.push(parse_window_order_by_item(inner_pair)?);
            }
            Rule::order_by_list => {
                for item_pair in inner_pair.into_inner() {
                    if item_pair.as_rule() == Rule::order_by_item {
                        order_by.push(parse_window_order_by_item(item_pair)?);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_window_order_by_item(pair: pest::iterators::Pair<Rule>) -> SqlResult<WindowOrderByItem> {
    let mut expr = None;
    let mut order = Order::Asc;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::expression => {
                expr = Some(parse_any_expression(inner_pair)?);
            }
            Rule::KW_ASC => order = Order::Asc,
            Rule::KW_DESC => order = Order::Desc,
            _ => {}
        }
    }

    let expr =
        expr.ok_or_else(|| SqlError::Parse("Missing expression in ORDER BY item".to_string()))?;

    Ok(WindowOrderByItem { expr, order })
}

fn parse_window_frame(pair: pest::iterators::Pair<Rule>) -> SqlResult<Box<WindowFrame>> {
    let mut units = FrameUnits::Rows;
    let mut start = FrameBound::UnboundedPreceding;
    let mut end = FrameBound::CurrentRow;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::KW_ROWS => units = FrameUnits::Rows,
            Rule::KW_RANGE => units = FrameUnits::Range,
            Rule::window_frame_extent => {
                let (s, e) = parse_window_frame_extent(inner_pair)?;
                start = s;
                end = e;
            }
            _ => {}
        }
    }

    Ok(Box::new(WindowFrame {
        units,
        start: Box::new(start),
        end: Box::new(end),
    }))
}

fn parse_window_frame_extent(
    pair: pest::iterators::Pair<Rule>,
) -> SqlResult<(FrameBound, FrameBound)> {
    let mut inner = pair.into_inner().peekable();

    if inner.peek().map(|p| p.as_rule()) == Some(Rule::KW_BETWEEN) {
        inner.next();

        let start = parse_window_frame_bound(
            inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing start bound".to_string()))?,
        )?;

        let end = if inner.peek().map(|p| p.as_rule()) == Some(Rule::KW_AND) {
            inner.next();
            parse_window_frame_bound(
                inner
                    .next()
                    .ok_or_else(|| SqlError::Parse("Missing end bound".to_string()))?,
            )?
        } else {
            FrameBound::CurrentRow
        };

        Ok((start, end))
    } else {
        let bound = parse_window_frame_bound(
            inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing frame bound".to_string()))?,
        )?;
        Ok((bound.clone(), bound))
    }
}

fn parse_window_frame_bound(pair: pest::iterators::Pair<Rule>) -> SqlResult<FrameBound> {
    let bound_str = pair.as_str().to_string();
    let mut value = None;
    let mut kind = None;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::KW_UNBOUNDED => kind = Some("unbounded".to_string()),
            Rule::KW_PRECEDING => kind = Some("preceding".to_string()),
            Rule::KW_FOLLOWING => kind = Some("following".to_string()),
            Rule::KW_CURRENT => kind = Some("current".to_string()),
            Rule::expression => {
                value = Some(parse_any_expression(inner_pair)?);
            }
            _ => {}
        }
    }

    let kind = kind.ok_or_else(|| SqlError::Parse("Missing frame bound kind".to_string()))?;

    match kind.as_str() {
        "unbounded" => {
            if bound_str.contains("PRECEDING") {
                Ok(FrameBound::UnboundedPreceding)
            } else {
                Ok(FrameBound::UnboundedFollowing)
            }
        }
        "current" => Ok(FrameBound::CurrentRow),
        "preceding" => {
            if let Some(expr) = value {
                Ok(FrameBound::Preceding(Box::new(expr)))
            } else {
                Err(SqlError::Parse("Missing preceding value".to_string()))
            }
        }
        "following" => {
            if let Some(expr) = value {
                Ok(FrameBound::Following(Box::new(expr)))
            } else {
                Err(SqlError::Parse("Missing following value".to_string()))
            }
        }
        _ => Err(SqlError::Parse(format!(
            "Unknown frame bound kind: {}",
            kind
        ))),
    }
}
