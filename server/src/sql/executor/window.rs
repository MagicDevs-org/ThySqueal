use crate::sql::error::SqlResult;
use crate::sql::eval::evaluate_expression_joined;
use crate::sql::executor::select::project::JoinedContext;
use crate::squeal::{Expression, WindowFuncType, WindowFunction};
use crate::storage::{Row, Table, Value};

pub struct WindowFunctionEvaluator;

#[derive(Clone)]
struct Partition {
    rows: Vec<usize>,
    sorted_indices: Vec<usize>,
}

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

        let partitions = self.compute_all_partitions(
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

    fn compute_all_partitions(
        &self,
        window_columns: &[(usize, &WindowFunction)],
        all_rows: &[JoinedContext<'_>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<Vec<Partition>> {
        let mut partitions = Vec::new();

        for &(_, wf) in window_columns {
            if wf.partition_by.is_empty() {
                let partition_rows: Vec<usize> = (0..all_rows.len()).collect();
                let sorted_indices = self.sort_partition_by_order_by(
                    wf,
                    &partition_rows,
                    all_rows,
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )?;
                partitions.push(Partition {
                    rows: partition_rows,
                    sorted_indices,
                });
            } else {
                let mut current_partition_start = 0;
                while current_partition_start < all_rows.len() {
                    let partition_end = self.find_partition_end(
                        wf,
                        all_rows,
                        current_partition_start,
                        params,
                        outer_contexts,
                        db_state,
                        executor,
                    )?;
                    let partition_rows: Vec<usize> =
                        (current_partition_start..partition_end).collect();
                    let sorted_indices = self.sort_partition_by_order_by(
                        wf,
                        &partition_rows,
                        all_rows,
                        params,
                        outer_contexts,
                        db_state,
                        executor,
                    )?;
                    partitions.push(Partition {
                        rows: partition_rows,
                        sorted_indices,
                    });
                    current_partition_start = partition_end;
                }
            }
        }

        Ok(partitions)
    }

    #[allow(clippy::too_many_arguments)]
    fn find_partition_end(
        &self,
        wf: &WindowFunction,
        all_rows: &[JoinedContext<'_>],
        start: usize,
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<usize> {
        let partition_key = self.compute_partition_key(
            wf,
            &all_rows[start],
            params,
            outer_contexts,
            db_state,
            executor,
        )?;

        for i in (start + 1)..all_rows.len() {
            let current_key = self.compute_partition_key(
                wf,
                &all_rows[i],
                params,
                outer_contexts,
                db_state,
                executor,
            )?;
            if current_key != partition_key {
                return Ok(i);
            }
        }
        Ok(all_rows.len())
    }

    fn compute_partition_key(
        &self,
        wf: &WindowFunction,
        ctx: &JoinedContext<'_>,
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<Vec<Value>> {
        let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = ctx
            .iter()
            .map(|(t, a, r)| (*t, a.as_deref(), r))
            .collect::<Vec<_>>();
        let eval_ctx =
            crate::sql::eval::EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state);

        wf.partition_by
            .iter()
            .map(|expr| evaluate_expression_joined(executor, expr, &eval_ctx))
            .collect::<SqlResult<Vec<_>>>()
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
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<Value> {
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
                let rank = self.compute_rank(
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
                let rank = self.compute_dense_rank(
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
                let rank = self.compute_rank(
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
                let count = self.compute_cume_dist(
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
                self.evaluate_for_row(
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
                self.evaluate_for_row(
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
                    self.evaluate_for_row(
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
                    self.evaluate_for_row(
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

    #[allow(clippy::too_many_arguments)]
    fn find_row_partition(
        &self,
        _wf: &WindowFunction,
        partitions: &[Partition],
        row_idx: usize,
    ) -> SqlResult<Partition> {
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

    #[allow(clippy::too_many_arguments)]
    fn sort_partition_by_order_by(
        &self,
        wf: &WindowFunction,
        partition_indices: &[usize],
        all_rows: &[JoinedContext<'_>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<Vec<usize>> {
        let mut indices_with_values: Vec<(usize, Vec<(Value, bool)>)> = Vec::new();

        for &idx in partition_indices {
            let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = all_rows[idx]
                .iter()
                .map(|(t, a, r)| (*t, a.as_deref(), r))
                .collect::<Vec<_>>();
            let eval_ctx = crate::sql::eval::EvalContext::new(
                &eval_ctx_list,
                params,
                outer_contexts,
                db_state,
            );

            let mut sort_values = Vec::new();
            for order_item in &wf.order_by {
                let val = evaluate_expression_joined(executor, &order_item.expr, &eval_ctx)?;
                sort_values.push((val, order_item.ascending));
            }
            indices_with_values.push((idx, sort_values));
        }

        indices_with_values.sort_by(|a, b| {
            for ((val_a, asc_a), (val_b, _asc)) in a.1.iter().zip(b.1.iter()) {
                match val_a.partial_cmp(val_b) {
                    Some(std::cmp::Ordering::Equal) => continue,
                    Some(order) => {
                        return if *asc_a { order } else { order.reverse() };
                    }
                    None => return std::cmp::Ordering::Equal,
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(indices_with_values
            .into_iter()
            .map(|(idx, _)| idx)
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_rank(
        &self,
        sorted_indices: &[usize],
        sorted_position: usize,
        wf: &WindowFunction,
        all_rows: &[JoinedContext<'_>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<i64> {
        if sorted_indices.is_empty() {
            return Ok(1);
        }

        let current_val = self.evaluate_order_by_values(
            wf,
            &all_rows[sorted_indices[sorted_position]],
            params,
            outer_contexts,
            db_state,
            executor,
        )?;

        let mut rank = 1i64;
        for i in 0..sorted_position {
            let other_val = self.evaluate_order_by_values(
                wf,
                &all_rows[sorted_indices[i]],
                params,
                outer_contexts,
                db_state,
                executor,
            )?;
            if other_val > current_val {
                rank += 1;
            }
        }
        Ok(rank)
    }

    fn evaluate_order_by_values(
        &self,
        wf: &WindowFunction,
        ctx: &JoinedContext<'_>,
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<Value> {
        if !wf.order_by.is_empty() {
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
            let val = evaluate_expression_joined(executor, &wf.order_by[0].expr, &eval_ctx)?;
            return Ok(val);
        }
        self.evaluate_for_row(&wf.args, ctx, params, outer_contexts, db_state, executor)
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_dense_rank(
        &self,
        sorted_indices: &[usize],
        sorted_position: usize,
        wf: &WindowFunction,
        all_rows: &[JoinedContext<'_>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<i64> {
        if sorted_indices.is_empty() {
            return Ok(1);
        }
        let current_val = self.evaluate_order_by_values(
            wf,
            &all_rows[sorted_indices[sorted_position]],
            params,
            outer_contexts,
            db_state,
            executor,
        )?;

        let mut rank = 1i64;
        let mut seen_higher_values: Vec<Value> = Vec::new();
        for i in 0..sorted_position {
            let other_val = self.evaluate_order_by_values(
                wf,
                &all_rows[sorted_indices[i]],
                params,
                outer_contexts,
                db_state,
                executor,
            )?;
            if other_val > current_val && !seen_higher_values.contains(&other_val) {
                seen_higher_values.push(other_val);
                rank += 1;
            }
        }
        Ok(rank)
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_cume_dist(
        &self,
        sorted_indices: &[usize],
        sorted_position: usize,
        wf: &WindowFunction,
        all_rows: &[JoinedContext<'_>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        executor: &dyn crate::sql::eval::Evaluator,
    ) -> SqlResult<usize> {
        if sorted_indices.is_empty() {
            return Ok(0);
        }
        let current_val = self.evaluate_order_by_values(
            wf,
            &all_rows[sorted_indices[sorted_position]],
            params,
            outer_contexts,
            db_state,
            executor,
        )?;

        let mut count = 0;
        for i in sorted_indices.iter() {
            let other_val = self.evaluate_order_by_values(
                wf,
                &all_rows[*i],
                params,
                outer_contexts,
                db_state,
                executor,
            )?;
            if other_val <= current_val {
                count += 1;
            }
        }
        Ok(count)
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
