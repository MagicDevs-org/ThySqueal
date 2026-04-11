pub mod functions;
pub mod partition;
pub mod sort;

use crate::squeal::eval::evaluate_expression_joined;
use crate::squeal::exec::ExecResult;
use crate::squeal::exec::select::project::JoinedContext;
use crate::squeal::ir::{Expression, WindowFuncType, WindowFunction};
use crate::storage::{Row, Table, Value};

use functions::{compute_cume_dist, compute_dense_rank, compute_rank, evaluate_for_row};
use partition::{Partition, compute_all_partitions};
use sort::sort_partition_by_order_by;

pub struct WindowFunctionEvaluator;

impl WindowFunctionEvaluator {
    pub fn evaluate_window_functions(
        &self,
        columns: &[crate::squeal::ir::SelectColumn],
        rows: &[JoinedContext<'_>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::squeal::eval::Evaluator,
    ) -> ExecResult<Vec<Vec<Value>>> {
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

        let partitions = compute_all_partitions(
            &window_columns,
            rows,
            params,
            outer_contexts,
            db_state,
            executor,
        )?;

        let mut results = Vec::new();

        for (row_idx, ctx) in rows.iter().enumerate() {
            let mut row_values = Vec::new();

            for col in columns.iter() {
                match &col.expr {
                    Expression::WindowFunc(wf) => {
                        let result = self.evaluate_window_function_for_row(
                            wf,
                            rows,
                            row_idx,
                            &partitions,
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
                        let eval_ctx = crate::squeal::eval::EvalContext::new(
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
    fn evaluate_window_function_for_row(
        &self,
        wf: &WindowFunction,
        all_rows: &[JoinedContext<'_>],
        current_row_idx: usize,
        partitions: &[Partition],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::squeal::eval::Evaluator,
    ) -> ExecResult<Value> {
        let partition = self.find_row_partition(wf, partitions, current_row_idx)?;

        if partition.rows.is_empty() {
            return Ok(Value::Null);
        }

        let sorted_position = partition
            .sorted_indices
            .iter()
            .position(|&idx| idx == current_row_idx)
            .unwrap_or(0);

        match wf.func_type {
            WindowFuncType::RowNumber => Ok(Value::Int(sorted_position as i64 + 1)),
            WindowFuncType::Rank => {
                let rank = compute_rank(
                    &partition.sorted_indices,
                    sorted_position,
                    wf,
                    all_rows,
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )?;
                Ok(Value::Int(rank))
            }
            WindowFuncType::DenseRank => {
                let rank = compute_dense_rank(
                    &partition.sorted_indices,
                    sorted_position,
                    wf,
                    all_rows,
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )?;
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
                let total_rows = partition.sorted_indices.len() as i64;
                if total_rows == 0 || n == 0 {
                    return Ok(Value::Int(1));
                }
                let bucket_size = (total_rows + n - 1) / n;
                let bucket = (sorted_position as i64 / bucket_size) + 1;
                Ok(Value::Int(bucket.min(n)))
            }
            WindowFuncType::PercentRank => {
                let rank = compute_rank(
                    &partition.sorted_indices,
                    sorted_position,
                    wf,
                    all_rows,
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )?;
                let total_rows = partition.sorted_indices.len();
                if total_rows <= 1 {
                    Ok(Value::Float(0.0))
                } else {
                    Ok(Value::Float(
                        (rank as f64 - 1.0) / (total_rows as f64 - 1.0),
                    ))
                }
            }
            WindowFuncType::CumeDist => {
                let count = compute_cume_dist(
                    &partition.sorted_indices,
                    sorted_position,
                    wf,
                    all_rows,
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )?;
                let total_rows = partition.sorted_indices.len();
                Ok(Value::Float(count as f64 / total_rows as f64))
            }
            WindowFuncType::FirstValue => {
                if partition.sorted_indices.is_empty() {
                    return Ok(Value::Null);
                }
                evaluate_for_row(
                    &wf.args,
                    &all_rows[partition.sorted_indices[0]],
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )
            }
            WindowFuncType::LastValue => {
                if partition.sorted_indices.is_empty() {
                    return Ok(Value::Null);
                }
                evaluate_for_row(
                    &wf.args,
                    &all_rows[partition.sorted_indices[sorted_position]],
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
                if n > 0 && n <= partition.sorted_indices.len() {
                    evaluate_for_row(
                        &wf.args,
                        &all_rows[partition.sorted_indices[n - 1]],
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
                let target_pos = sorted_position as i64 + (if is_lag { -offset } else { offset });

                if target_pos < 0 || target_pos as usize >= partition.sorted_indices.len() {
                    Ok(Value::Null)
                } else {
                    let target_idx = partition.sorted_indices[target_pos as usize];
                    evaluate_for_row(
                        &wf.args,
                        &all_rows[target_idx],
                        params,
                        outer_contexts,
                        db_state,
                        executor,
                    )
                }
            }
        }
    }

    fn find_row_partition(
        &self,
        _wf: &WindowFunction,
        partitions: &[Partition],
        row_idx: usize,
    ) -> ExecResult<Partition> {
        for partition in partitions {
            if partition.rows.contains(&row_idx) {
                return Ok(partition.to_owned());
            }
        }
        Ok(Partition {
            rows: vec![row_idx],
            sorted_indices: vec![row_idx],
        })
    }

    fn project_columns(
        &self,
        columns: &[crate::squeal::ir::SelectColumn],
        rows: &[JoinedContext<'_>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::squeal::eval::Evaluator,
    ) -> ExecResult<Vec<Vec<Value>>> {
        let mut results = Vec::new();

        for ctx in rows {
            let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = ctx
                .iter()
                .map(|(t, a, r)| (*t, a.as_deref(), r))
                .collect::<Vec<_>>();
            let eval_ctx = crate::squeal::eval::EvalContext::new(
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
