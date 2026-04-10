use crate::engines::mysql::error::SqlResult;
use crate::engines_mysql::eval::evaluate_expression_joined;
use crate::engines_mysql::executor::select::project::JoinedContext;
use crate::squeal::ir::{Expression, WindowFunction};
use crate::storage::{Row, Table, Value};

#[allow(clippy::too_many_arguments)]
pub fn compute_rank(
    sorted_indices: &[usize],
    sorted_position: usize,
    wf: &WindowFunction,
    all_rows: &[JoinedContext<'_>],
    params: &[Value],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &crate::storage::DatabaseState,
    executor: &dyn crate::engines_mysql::eval::Evaluator,
) -> SqlResult<i64> {
    if sorted_indices.is_empty() {
        return Ok(1);
    }

    let current_val = evaluate_order_by_values(
        wf,
        &all_rows[sorted_indices[sorted_position]],
        params,
        outer_contexts,
        db_state,
        executor,
    )?;

    let mut rank = 1i64;
    for i in 0..sorted_position {
        let other_val = evaluate_order_by_values(
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

pub fn evaluate_order_by_values(
    wf: &WindowFunction,
    ctx: &JoinedContext<'_>,
    params: &[Value],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &crate::storage::DatabaseState,
    executor: &dyn crate::engines_mysql::eval::Evaluator,
) -> SqlResult<Value> {
    if !wf.order_by.is_empty() {
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
        let val = evaluate_expression_joined(executor, &wf.order_by[0].expr, &eval_ctx)?;
        return Ok(val);
    }
    evaluate_for_row(&wf.args, ctx, params, outer_contexts, db_state, executor)
}

#[allow(clippy::too_many_arguments)]
pub fn compute_dense_rank(
    sorted_indices: &[usize],
    sorted_position: usize,
    wf: &WindowFunction,
    all_rows: &[JoinedContext<'_>],
    params: &[Value],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &crate::storage::DatabaseState,
    executor: &dyn crate::engines_mysql::eval::Evaluator,
) -> SqlResult<i64> {
    if sorted_indices.is_empty() {
        return Ok(1);
    }
    let current_val = evaluate_order_by_values(
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
        let other_val = evaluate_order_by_values(
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
pub fn compute_cume_dist(
    sorted_indices: &[usize],
    sorted_position: usize,
    wf: &WindowFunction,
    all_rows: &[JoinedContext<'_>],
    params: &[Value],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &crate::storage::DatabaseState,
    executor: &dyn crate::engines_mysql::eval::Evaluator,
) -> SqlResult<usize> {
    if sorted_indices.is_empty() {
        return Ok(0);
    }
    let current_val = evaluate_order_by_values(
        wf,
        &all_rows[sorted_indices[sorted_position]],
        params,
        outer_contexts,
        db_state,
        executor,
    )?;

    let mut count = 0;
    for i in sorted_indices.iter() {
        let other_val = evaluate_order_by_values(
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

pub fn evaluate_for_row(
    args: &[Expression],
    ctx: &JoinedContext<'_>,
    params: &[Value],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &crate::storage::DatabaseState,
    executor: &dyn crate::engines_mysql::eval::Evaluator,
) -> SqlResult<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

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

    evaluate_expression_joined(executor, &args[0], &eval_ctx)
}
