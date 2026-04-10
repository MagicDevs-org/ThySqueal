pub mod base;
pub mod cte;
pub mod join;
pub mod project;

use crate::engines::mysql::error::SqlResult;
use crate::squeal;
use crate::squeal::eval::{EvalContext, evaluate_condition_joined, evaluate_expression_joined};
use crate::squeal::exec::window::WindowFunctionEvaluator;
use crate::squeal::exec::{Executor, QueryResult, SelectQueryPlan};
use crate::squeal::ir::{Expression, OrderByItem, SetOperationClause, SetOperator};
use crate::storage::{Row, Table};

use futures::FutureExt;
use futures::future::BoxFuture;

use project::JoinedContext;

impl Executor {
    pub fn exec_select_recursive<'a>(
        &'a self,
        plan: SelectQueryPlan<'a>,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        async move {
            let stmt = &plan.stmt;
            let outer_contexts = plan.outer_contexts;
            let params = plan.params;
            let db_state = plan.db_state;
            let session = plan.session.clone();

            // 0. Resolve CTEs
            let cte_tables = self.resolve_ctes(&plan).await?;

            // 1. Resolve base table and initial rows
            let (base_resolved, initial_rows) = self.resolve_base_table(&plan, &cte_tables)?;
            let base_table = base_resolved.table();

            let base_alias_owned = stmt.table_alias.clone();

            let joined_rows = initial_rows
                .into_iter()
                .map(|r| vec![(base_table, base_alias_owned.clone(), r)])
                .collect();

            // 3. Process JOINS
            let joined_rows = self.process_joins(&plan, &cte_tables, joined_rows)?;

            // 4. Apply WHERE
            let mut matched_rows = Vec::new();
            if let Some(ref where_cond) = stmt.where_clause {
                for ctx in joined_rows {
                    let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> =
                        ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
                    let eval_ctx =
                        EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state)
                            .with_session(&session);
                    if evaluate_condition_joined(self, where_cond, &eval_ctx)? {
                        matched_rows.push(ctx);
                    }
                }
            } else {
                matched_rows = joined_rows;
            }

            // 5. Handle Aggregates and Grouping
            let has_aggregates = stmt
                .columns
                .iter()
                .any(|c| matches!(c.expr, Expression::FunctionCall(_)));

            let has_window_funcs = stmt
                .columns
                .iter()
                .any(|c| matches!(c.expr, Expression::WindowFunc(_)));

            if has_aggregates || !stmt.group_by.is_empty() {
                let group_plan = SelectQueryPlan::new(stmt.clone(), db_state, session);
                return self
                    .exec_select_with_grouping_owned(group_plan, matched_rows, &cte_tables)
                    .await;
            }

            // 5b. Evaluate Window Functions
            if has_window_funcs {
                let window_evaluator = WindowFunctionEvaluator;

                // Sort rows by ORDER BY for window frame evaluation
                let mut sorted_rows: Vec<JoinedContext> = matched_rows;
                if !stmt.order_by.is_empty() {
                    self.apply_order_by(
                        &stmt.order_by,
                        &mut sorted_rows,
                        params,
                        outer_contexts,
                        db_state,
                        &session,
                    )?;
                }

                // Apply LIMIT
                let final_rows: Vec<JoinedContext> = if let Some(ref limit) = stmt.limit {
                    let offset = limit.offset.unwrap_or(0);
                    sorted_rows
                        .into_iter()
                        .skip(offset)
                        .take(limit.count)
                        .collect()
                } else {
                    sorted_rows
                };

                // Evaluate window functions with final rows
                let window_results = window_evaluator.evaluate_window_functions(
                    &stmt.columns,
                    &final_rows,
                    params,
                    outer_contexts,
                    db_state,
                    self,
                )?;

                let result_columns: Vec<String> = self.get_result_column_names(
                    stmt,
                    base_table,
                    &stmt.joins,
                    db_state,
                    &cte_tables,
                );

                let mut projected_rows = window_results;
                if stmt.distinct {
                    let mut seen = std::collections::HashSet::new();
                    projected_rows.retain(|row| seen.insert(row.clone()));
                }

                return Ok(QueryResult {
                    columns: result_columns,
                    rows: projected_rows,
                    rows_affected: 0,
                    transaction_id: session.transaction_id,
                    session: None,
                });
            }

            // 5c. Handle Set Operations
            if !stmt.set_operations.is_empty() {
                let result_columns: Vec<String> = self.get_result_column_names(
                    stmt,
                    base_table,
                    &stmt.joins,
                    db_state,
                    &cte_tables,
                );

                let mut projected_rows = Vec::new();
                for ctx in matched_rows {
                    let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> =
                        ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
                    let eval_ctx =
                        EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state)
                            .with_session(&session);
                    let mut row_values = Vec::new();
                    for col in &stmt.columns {
                        match &col.expr {
                            Expression::Star => {
                                for (_table, _alias, row) in &ctx {
                                    row_values.extend(row.values.clone());
                                }
                            }
                            _ => {
                                row_values
                                    .push(evaluate_expression_joined(self, &col.expr, &eval_ctx)?);
                            }
                        }
                    }
                    projected_rows.push(row_values);
                }

                let initial_result = QueryResult {
                    columns: result_columns,
                    rows: projected_rows,
                    rows_affected: 0,
                    transaction_id: session.transaction_id.clone(),
                    session: None,
                };

                return self.exec_set_operations(
                    initial_result,
                    &stmt.set_operations,
                    db_state,
                    session,
                    params,
                    outer_contexts,
                );
            }

            // 6. Apply ORDER BY
            if !stmt.order_by.is_empty() {
                self.apply_order_by(
                    &stmt.order_by,
                    &mut matched_rows,
                    params,
                    outer_contexts,
                    db_state,
                    &session,
                )?;
            }

            // 7. Apply LIMIT and OFFSET
            let final_rows = if let Some(ref limit) = stmt.limit {
                let offset = limit.offset.unwrap_or(0);
                matched_rows
                    .into_iter()
                    .skip(offset)
                    .take(limit.count)
                    .collect()
            } else {
                matched_rows
            };

            // 8. Project Columns
            let result_columns: Vec<String> =
                self.get_result_column_names(stmt, base_table, &stmt.joins, db_state, &cte_tables);

            let mut projected_rows = Vec::new();
            for ctx in final_rows {
                let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> =
                    ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
                let eval_ctx = EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state)
                    .with_session(&session);
                let mut row_values = Vec::new();
                for col in &stmt.columns {
                    match &col.expr {
                        Expression::Star => {
                            for (_table, _alias, row) in &ctx {
                                row_values.extend(row.values.clone());
                            }
                        }
                        _ => {
                            row_values
                                .push(evaluate_expression_joined(self, &col.expr, &eval_ctx)?);
                        }
                    }
                }
                projected_rows.push(row_values);
            }

            if stmt.distinct {
                let mut seen = std::collections::HashSet::new();
                projected_rows.retain(|row| seen.insert(row.clone()));
            }

            Ok(QueryResult {
                columns: result_columns,
                rows: projected_rows,
                rows_affected: 0,
                transaction_id: session.transaction_id,
                session: None,
            })
        }
        .boxed()
    }

    fn exec_set_operations(
        &self,
        initial_result: QueryResult,
        set_ops: &[SetOperationClause],
        db_state: &crate::storage::DatabaseState,
        session: crate::squeal::exec::Session,
        _params: &[crate::storage::Value],
        _outer_contexts: &[(&crate::storage::Table, Option<&str>, &crate::storage::Row)],
    ) -> SqlResult<QueryResult> {
        let mut result = initial_result;

        for set_op in set_ops {
            let select_plan =
                SelectQueryPlan::new((*set_op.select).clone(), db_state, session.clone());
            let next_result = futures::executor::block_on(self.exec_select_recursive(select_plan))?;

            result = match &set_op.operator {
                SetOperator::Union => self.set_union(&result, &next_result, true)?,
                SetOperator::UnionAll => self.set_union(&result, &next_result, false)?,
                SetOperator::Intersect => self.set_intersect(&result, &next_result)?,
                SetOperator::Except => self.set_except(&result, &next_result)?,
            };
        }

        Ok(result)
    }

    fn set_union(
        &self,
        a: &QueryResult,
        b: &QueryResult,
        distinct: bool,
    ) -> SqlResult<QueryResult> {
        let mut rows = a.rows.clone();
        rows.extend(b.rows.clone());

        if distinct {
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| seen.insert(row.clone()));
        }

        Ok(QueryResult {
            columns: a.columns.clone(),
            rows,
            rows_affected: 0,
            transaction_id: a.transaction_id.clone(),
            session: a.session.clone(),
        })
    }

    fn set_intersect(&self, a: &QueryResult, b: &QueryResult) -> SqlResult<QueryResult> {
        let b_rows: std::collections::HashSet<_> = b.rows.iter().collect();
        let rows: Vec<_> = a
            .rows
            .iter()
            .filter(|row| b_rows.contains(row))
            .cloned()
            .collect();

        Ok(QueryResult {
            columns: a.columns.clone(),
            rows,
            rows_affected: 0,
            transaction_id: a.transaction_id.clone(),
            session: a.session.clone(),
        })
    }

    fn set_except(&self, a: &QueryResult, b: &QueryResult) -> SqlResult<QueryResult> {
        let b_rows: std::collections::HashSet<_> = b.rows.iter().collect();
        let rows: Vec<_> = a
            .rows
            .iter()
            .filter(|row| !b_rows.contains(row))
            .cloned()
            .collect();

        Ok(QueryResult {
            columns: a.columns.clone(),
            rows,
            rows_affected: 0,
            transaction_id: a.transaction_id.clone(),
            session: a.session.clone(),
        })
    }

    fn apply_order_by(
        &self,
        order_by: &[OrderByItem],
        rows: &mut Vec<JoinedContext>,
        params: &[crate::storage::Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &crate::storage::DatabaseState,
        session: &crate::squeal::exec::Session,
    ) -> SqlResult<()> {
        let mut err = None;
        rows.sort_by(|a, b| {
            let eval_ctx_list_a: Vec<(&Table, Option<&str>, &Row)> =
                a.iter().map(|(t, al, r)| (*t, al.as_deref(), r)).collect();
            let eval_ctx_list_b: Vec<(&Table, Option<&str>, &Row)> =
                b.iter().map(|(t, al, r)| (*t, al.as_deref(), r)).collect();

            let eval_ctx_a = EvalContext::new(&eval_ctx_list_a, params, outer_contexts, db_state)
                .with_session(session);

            let eval_ctx_b = EvalContext::new(&eval_ctx_list_b, params, outer_contexts, db_state)
                .with_session(session);

            for item in order_by {
                let val_a = match evaluate_expression_joined(self, &item.expr, &eval_ctx_a) {
                    Ok(v) => v,
                    Err(e) => {
                        err = Some(e);
                        return std::cmp::Ordering::Equal;
                    }
                };
                let val_b = match evaluate_expression_joined(self, &item.expr, &eval_ctx_b) {
                    Ok(v) => v,
                    Err(e) => {
                        err = Some(e);
                        return std::cmp::Ordering::Equal;
                    }
                };

                if let Some(ord) = val_a.partial_cmp(&val_b)
                    && ord != std::cmp::Ordering::Equal
                {
                    return if item.order == squeal::ir::Order::Desc {
                        ord.reverse()
                    } else {
                        ord
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
        if let Some(e) = err {
            return Err(e);
        }
        Ok(())
    }
}
