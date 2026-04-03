use crate::sql::error::SqlResult;
use crate::sql::eval::evaluate_expression_joined;
use crate::sql::executor::select::project::JoinedContext;
use crate::squeal::{Expression, WindowFuncType, WindowFunction};
use crate::storage::{Row, Table, Value};

pub struct WindowFunctionEvaluator;

impl WindowFunctionEvaluator {
    pub fn evaluate_window_functions(
        &self,
        columns: &[crate::squeal::SelectColumn],
        rows: &[JoinedContext<'_>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<Vec<Vec<Value>>> {
        let window_columns: Vec<(usize, &WindowFunction)> = columns
            .iter()
            .enumerate()
            .filter_map(|(i, col)| {
                if let Expression::WindowFunc(wf) = &col.expr {
                    Some((i, wf))
                } else {
                    None
                }
            })
            .collect();

        if window_columns.is_empty() {
            return self.project_columns(columns, rows, params, outer_contexts, db_state, executor);
        }

        let mut results = Vec::new();

        for (row_idx, ctx) in rows.iter().enumerate() {
            let mut row_values = Vec::new();

            for col in columns.iter() {
                match &col.expr {
                    Expression::WindowFunc(wf) => {
                        let result = self.evaluate_window_function(
                            wf,
                            rows,
                            row_idx,
                            params,
                            outer_contexts,
                            db_state,
                            executor,
                        )?;
                        row_values.push(result);
                    }
                    _ => {
                        let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = ctx
                            .iter()
                            .map(|(t, a, r)| (*t, a.as_deref(), r))
                            .collect::<Vec<_>>();
                        let eval_ctx = crate::sql::eval::EvalContext::new(
                            &eval_ctx_list,
                            params,
                            outer_contexts,
                            db_state,
                        );
                        row_values
                            .push(evaluate_expression_joined(executor, &col.expr, &eval_ctx)?);
                    }
                }
            }
            results.push(row_values);
        }

        Ok(results)
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_window_function(
        &self,
        wf: &WindowFunction,
        all_rows: &[JoinedContext<'_>],
        current_row_idx: usize,
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<Value> {
        let partition_rows =
            self.compute_partition(wf, all_rows, params, outer_contexts, db_state, executor)?;

        match wf.func_type {
            WindowFuncType::RowNumber => Ok(Value::Int(current_row_idx as i64 + 1)),
            WindowFuncType::Rank => {
                let val = self.evaluate_for_row(
                    &wf.args,
                    &all_rows[current_row_idx],
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )?;
                let mut rank = 1;
                for (i, _) in partition_rows.iter().enumerate() {
                    if i >= current_row_idx {
                        break;
                    }
                    let other_val = self.evaluate_for_row(
                        &wf.args,
                        &all_rows[i],
                        params,
                        outer_contexts,
                        db_state,
                        executor,
                    )?;
                    if other_val < val {
                        rank += 1;
                    }
                }
                Ok(Value::Int(rank))
            }
            WindowFuncType::DenseRank => {
                let val = self.evaluate_for_row(
                    &wf.args,
                    &all_rows[current_row_idx],
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )?;
                let mut rank = 1;
                for (i, _) in partition_rows.iter().enumerate() {
                    if i >= current_row_idx {
                        break;
                    }
                    let other_val = self.evaluate_for_row(
                        &wf.args,
                        &all_rows[i],
                        params,
                        outer_contexts,
                        db_state,
                        executor,
                    )?;
                    if other_val < val {
                        rank += 1;
                    }
                }
                Ok(Value::Int(rank))
            }
            WindowFuncType::Ntile => {
                let n = wf
                    .args
                    .first()
                    .and_then(|a| match a {
                        Expression::Literal(Value::Int(n)) => Some(*n),
                        _ => None,
                    })
                    .unwrap_or(1);
                let total_rows = partition_rows.len() as i64;
                if total_rows == 0 || n == 0 {
                    return Ok(Value::Int(1));
                }
                let bucket_size = (total_rows + n - 1) / n;
                let bucket = (current_row_idx as i64 / bucket_size) + 1;
                Ok(Value::Int(bucket.min(n)))
            }
            WindowFuncType::PercentRank => {
                let val = self.evaluate_for_row(
                    &wf.args,
                    &all_rows[current_row_idx],
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )?;
                let total_rows = partition_rows.len();
                let mut rank = 1;
                for (i, _) in partition_rows.iter().enumerate() {
                    if i >= current_row_idx {
                        break;
                    }
                    let other_val = self.evaluate_for_row(
                        &wf.args,
                        &all_rows[i],
                        params,
                        outer_contexts,
                        db_state,
                        executor,
                    )?;
                    if other_val < val {
                        rank += 1;
                    }
                }
                if total_rows <= 1 {
                    Ok(Value::Float(0.0))
                } else {
                    Ok(Value::Float(
                        (rank as f64 - 1.0) / (total_rows as f64 - 1.0),
                    ))
                }
            }
            WindowFuncType::CumeDist => {
                let val = self.evaluate_for_row(
                    &wf.args,
                    &all_rows[current_row_idx],
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )?;
                let total_rows = partition_rows.len();
                let mut count = 0;
                for (i, _) in partition_rows.iter().enumerate() {
                    let other_val = self.evaluate_for_row(
                        &wf.args,
                        &all_rows[i],
                        params,
                        outer_contexts,
                        db_state,
                        executor,
                    )?;
                    if other_val <= val {
                        count += 1;
                    }
                }
                Ok(Value::Float(count as f64 / total_rows as f64))
            }
            WindowFuncType::FirstValue => {
                if partition_rows.is_empty() {
                    return Ok(Value::Null);
                }
                self.evaluate_for_row(
                    &wf.args,
                    &all_rows[0],
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )
            }
            WindowFuncType::LastValue => {
                if partition_rows.is_empty() {
                    return Ok(Value::Null);
                }
                let last_idx = partition_rows.len() - 1;
                self.evaluate_for_row(
                    &wf.args,
                    &all_rows[last_idx],
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )
            }
            WindowFuncType::NthValue => {
                let n = wf
                    .args
                    .get(1)
                    .and_then(|a| match a {
                        Expression::Literal(Value::Int(n)) => Some(*n as usize),
                        _ => None,
                    })
                    .unwrap_or(1);
                if n > 0 && n <= partition_rows.len() {
                    self.evaluate_for_row(
                        &wf.args,
                        &all_rows[n - 1],
                        params,
                        outer_contexts,
                        db_state,
                        executor,
                    )
                } else {
                    Ok(Value::Null)
                }
            }
            WindowFuncType::Lag | WindowFuncType::Lead => {
                let offset = wf
                    .args
                    .get(1)
                    .and_then(|a| match a {
                        Expression::Literal(Value::Int(n)) => Some(*n),
                        _ => None,
                    })
                    .unwrap_or(1);
                let is_lag = matches!(wf.func_type, WindowFuncType::Lag);
                let target_idx = current_row_idx as i64 + (if is_lag { -offset } else { offset });

                if target_idx < 0 || target_idx as usize >= partition_rows.len() {
                    Ok(Value::Null)
                } else {
                    self.evaluate_for_row(
                        &wf.args,
                        &all_rows[target_idx as usize],
                        params,
                        outer_contexts,
                        db_state,
                        executor,
                    )
                }
            }
        }
    }

    fn compute_partition(
        &self,
        wf: &WindowFunction,
        all_rows: &[JoinedContext<'_>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<Vec<usize>> {
        if wf.partition_by.is_empty() {
            return Ok((0..all_rows.len()).collect());
        }

        let mut partition_indices: Vec<usize> = Vec::new();
        let mut current_partition_key: Option<Vec<Value>> = None;

        for (idx, row) in all_rows.iter().enumerate() {
            let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = row
                .iter()
                .map(|(t, a, r)| (*t, a.as_deref(), r))
                .collect::<Vec<_>>();
            let eval_ctx = crate::sql::eval::EvalContext::new(
                &eval_ctx_list,
                params,
                outer_contexts,
                db_state,
            );

            let partition_key: Vec<Value> = wf
                .partition_by
                .iter()
                .map(|expr| evaluate_expression_joined(executor, expr, &eval_ctx))
                .collect::<SqlResult<Vec<_>>>()?;

            if let Some(ref key) = current_partition_key {
                if key == &partition_key {
                    partition_indices.push(idx);
                } else {
                    return Ok(partition_indices);
                }
            } else {
                current_partition_key = Some(partition_key);
                partition_indices.push(idx);
            }
        }

        Ok(partition_indices)
    }

    fn evaluate_for_row(
        &self,
        args: &[Expression],
        ctx: &JoinedContext<'_>,
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }

        let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = ctx
            .iter()
            .map(|(t, a, r)| (*t, a.as_deref(), r))
            .collect::<Vec<_>>();
        let eval_ctx =
            crate::sql::eval::EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state);

        evaluate_expression_joined(executor, &args[0], &eval_ctx)
    }

    fn project_columns(
        &self,
        columns: &[crate::squeal::SelectColumn],
        rows: &[JoinedContext<'_>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<Vec<Vec<Value>>> {
        let mut results = Vec::new();

        for ctx in rows {
            let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = ctx
                .iter()
                .map(|(t, a, r)| (*t, a.as_deref(), r))
                .collect::<Vec<_>>();
            let eval_ctx = crate::sql::eval::EvalContext::new(
                &eval_ctx_list,
                params,
                outer_contexts,
                db_state,
            );

            let mut row_values = Vec::new();
            for col in columns {
                match &col.expr {
                    Expression::Star => {
                        for (_table, _alias, row) in ctx {
                            row_values.extend(row.values.clone());
                        }
                    }
                    _ => {
                        row_values
                            .push(evaluate_expression_joined(executor, &col.expr, &eval_ctx)?);
                    }
                }
            }
            results.push(row_values);
        }

        Ok(results)
    }
}
