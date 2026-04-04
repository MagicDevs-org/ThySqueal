use crate::sql::error::SqlResult;
use crate::sql::eval::evaluate_expression_joined;
use crate::sql::executor::select::project::JoinedContext;
use crate::squeal::WindowFunction;
use crate::storage::{Row, Table, Value};

#[allow(clippy::too_many_arguments)]
pub fn sort_partition_by_order_by(
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
        let eval_ctx =
            crate::sql::eval::EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state);

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
