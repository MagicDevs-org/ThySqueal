use crate::engines::mysql::error::SqlResult;
use crate::engines_mysql::eval::evaluate_expression_joined;
use crate::engines_mysql::executor::select::project::JoinedContext;
use crate::squeal::ir::WindowFunction;
use crate::storage::{Row, Table, Value};

#[derive(Clone)]
pub struct Partition {
    pub rows: Vec<usize>,
    pub sorted_indices: Vec<usize>,
}

pub fn compute_all_partitions(
    window_columns: &[(usize, &WindowFunction)],
    all_rows: &[JoinedContext<'_>],
    params: &[Value],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &crate::storage::DatabaseState,
    executor: &dyn crate::engines_mysql::eval::Evaluator,
) -> SqlResult<Vec<Partition>> {
    let mut partitions = Vec::new();

    for &(_, wf) in window_columns {
        if wf.partition_by.is_empty() {
            let partition_rows: Vec<usize> = (0..all_rows.len()).collect();
            let sorted_indices = super::sort_partition_by_order_by(
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
                let partition_end = find_partition_end(
                    wf,
                    all_rows,
                    current_partition_start,
                    params,
                    outer_contexts,
                    db_state,
                    executor,
                )?;
                let partition_rows: Vec<usize> = (current_partition_start..partition_end).collect();
                let sorted_indices = super::sort_partition_by_order_by(
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
pub fn find_partition_end(
    wf: &WindowFunction,
    all_rows: &[JoinedContext<'_>],
    start: usize,
    params: &[Value],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &crate::storage::DatabaseState,
    executor: &dyn crate::engines_mysql::eval::Evaluator,
) -> SqlResult<usize> {
    let partition_key = compute_partition_key(
        wf,
        &all_rows[start],
        params,
        outer_contexts,
        db_state,
        executor,
    )?;

    for (i, row) in all_rows.iter().enumerate().skip(start + 1) {
        let current_key =
            compute_partition_key(wf, row, params, outer_contexts, db_state, executor)?;
        if current_key != partition_key {
            return Ok(i);
        }
    }
    Ok(all_rows.len())
}

pub fn compute_partition_key(
    wf: &WindowFunction,
    ctx: &JoinedContext<'_>,
    params: &[Value],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &crate::storage::DatabaseState,
    executor: &dyn crate::engines_mysql::eval::Evaluator,
) -> SqlResult<Vec<Value>> {
    let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = ctx
        .iter()
        .map(|(t, a, r)| (*t, a.as_deref(), r))
        .collect::<Vec<_>>();
    let eval_ctx = crate::engines_mysql::eval::EvalContext::new(
        &eval_ctx_list,
        params,
        outer_contexts,
        db_state,
    );

    wf.partition_by
        .iter()
        .map(|expr| evaluate_expression_joined(executor, expr, &eval_ctx))
        .collect::<SqlResult<Vec<_>>>()
}
