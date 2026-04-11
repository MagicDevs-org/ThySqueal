use super::super::Executor;
use crate::squeal::eval::{EvalContext, evaluate_expression_joined};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::{AggregateType, Expression, FunctionCall};
use crate::storage::{DatabaseState, Row, Table, Value};

impl Executor {
    pub(crate) fn eval_aggregate_joined(
        &self,
        fc: &FunctionCall,
        contexts: &[Vec<(&Table, Option<&str>, &Row)>],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
        session: &super::super::Session,
    ) -> ExecResult<Value> {
        match fc.name {
            AggregateType::Count => {
                if fc.args.len() == 1 && matches!(fc.args[0], Expression::Star) {
                    Ok(Value::Int(contexts.len() as i64))
                } else {
                    let mut count = 0;
                    for ctx_list in contexts {
                        let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state)
                            .with_session(session);
                        let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                        if val != Value::Null {
                            count += 1;
                        }
                    }
                    Ok(Value::Int(count))
                }
            }
            AggregateType::Sum => {
                let mut sum_f = 0.0;
                let mut sum_i = 0;
                let mut is_float = false;
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state)
                        .with_session(session);
                    let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                    match val {
                        Value::Int(i) => {
                            sum_i += i;
                            sum_f += i as f64;
                        }
                        Value::Float(f) => {
                            sum_f += f;
                            is_float = true;
                        }
                        Value::Null => {}
                        _ => {
                            return Err(ExecError::TypeMismatch(
                                "SUM requires numeric values".to_string(),
                            ));
                        }
                    }
                }
                if is_float {
                    Ok(Value::Float(sum_f))
                } else {
                    Ok(Value::Int(sum_i))
                }
            }
            AggregateType::Min => {
                let mut min_val: Option<Value> = None;
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state)
                        .with_session(session);
                    let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                    if val == Value::Null {
                        continue;
                    }
                    if min_val.as_ref().is_none_or(|mv| &val < mv) {
                        min_val = Some(val);
                    }
                }
                Ok(min_val.unwrap_or(Value::Null))
            }
            AggregateType::Max => {
                let mut max_val: Option<Value> = None;
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state)
                        .with_session(session);
                    let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                    if val == Value::Null {
                        continue;
                    }
                    if max_val.as_ref().is_none_or(|mv| &val > mv) {
                        max_val = Some(val);
                    }
                }
                Ok(max_val.unwrap_or(Value::Null))
            }
            AggregateType::Avg => {
                let mut sum = 0.0;
                let mut count = 0;
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state)
                        .with_session(session);
                    let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                    match val {
                        Value::Int(i) => {
                            sum += i as f64;
                            count += 1;
                        }
                        Value::Float(f) => {
                            sum += f;
                            count += 1;
                        }
                        Value::Null => {}
                        _ => {
                            return Err(ExecError::TypeMismatch(
                                "AVG requires numeric values".to_string(),
                            ));
                        }
                    }
                }
                if count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(sum / count as f64))
                }
            }
            AggregateType::GroupConcat => {
                let mut values: Vec<String> = Vec::new();
                let separator = if fc.args.len() > 1 {
                    let eval_ctx =
                        EvalContext::new(&[], &[], outer_contexts, db_state).with_session(session);
                    match evaluate_expression_joined(self, &fc.args[1], &eval_ctx)? {
                        Value::Text(s) => s,
                        _ => ",".to_string(),
                    }
                } else {
                    ",".to_string()
                };
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state)
                        .with_session(session);
                    let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                    match val {
                        Value::Null => {}
                        Value::Text(s) => values.push(s),
                        Value::Int(i) => values.push(i.to_string()),
                        Value::Float(f) => values.push(f.to_string()),
                        _ => values.push(format!("{:?}", val)),
                    }
                }
                Ok(Value::Text(values.join(&separator)))
            }
            AggregateType::JsonArrayAgg => {
                let mut values: Vec<String> = Vec::new();
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state)
                        .with_session(session);
                    let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                    match val {
                        Value::Null => values.push("null".to_string()),
                        Value::Text(s) => values.push(format!(
                            "\"{}\"",
                            s.replace('\\', "\\\\").replace('"', "\\\"")
                        )),
                        Value::Int(i) => values.push(i.to_string()),
                        Value::Float(f) => values.push(f.to_string()),
                        Value::Bool(b) => values.push(b.to_string()),
                        _ => values.push(format!("\"{:?}\"", val)),
                    }
                }
                Ok(Value::Text(format!("[{}]", values.join(","))))
            }
            AggregateType::JsonObjectAgg => {
                if fc.args.len() < 2 {
                    return Err(ExecError::Parse(
                        "JSON_OBJECTAGG requires two arguments".to_string(),
                    ));
                }
                let mut map: std::collections::BTreeMap<String, String> =
                    std::collections::BTreeMap::new();
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state)
                        .with_session(session);
                    let key_val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                    let val_val = evaluate_expression_joined(self, &fc.args[1], &eval_ctx)?;
                    let key = match key_val {
                        Value::Text(s) => s,
                        Value::Int(i) => i.to_string(),
                        _ => continue,
                    };
                    let val_str = match val_val {
                        Value::Null => "null".to_string(),
                        Value::Text(s) => {
                            format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
                        }
                        Value::Int(i) => i.to_string(),
                        Value::Float(f) => f.to_string(),
                        Value::Bool(b) => b.to_string(),
                        _ => format!("\"{:?}\"", val_val),
                    };
                    map.insert(key, val_str);
                }
                let pairs: Vec<String> = map
                    .iter()
                    .map(|(k, v)| {
                        format!("\"{}\":{}", k.replace('\\', "\\\\").replace('"', "\\\""), v)
                    })
                    .collect();
                Ok(Value::Text(format!("{{{}}}", pairs.join(","))))
            }
        }
    }
}
