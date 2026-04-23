use super::project::JoinedContext;
use crate::squeal;
use crate::squeal::eval::{EvalContext, evaluate_condition_joined};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::exec::{Executor, SelectQueryPlan};
use crate::storage::{Row, Table};
use std::collections::HashMap;

impl Executor {
    pub fn process_joins<'b>(
        &self,
        plan: &SelectQueryPlan<'b>,
        cte_tables: &'b HashMap<String, Table>,
        mut joined_rows: Vec<JoinedContext<'b>>,
    ) -> ExecResult<Vec<JoinedContext<'b>>> {
        let stmt = &plan.stmt;
        let outer_contexts = plan.outer_contexts;
        let params = plan.params;
        let db_state = plan.db_state;

        for join in &stmt.joins {
            let join_table = if let Some(t) = cte_tables.get(&join.table) {
                t
            } else if join.table.starts_with("information_schema.") {
                return Err(ExecError::Runtime(
                    "JOIN with information_schema is not yet supported".to_string(),
                ));
            } else {
                db_state
                    .get_table(&join.table)
                    .ok_or_else(|| ExecError::TableNotFound(join.table.clone()))?
            };

            let join_alias = join.table_alias.clone();

            match join.join_type {
                squeal::ir::JoinType::Right => {
                    let right_rows = &join_table.data.rows;
                    let mut matching_rows = Vec::new();

                    for right_row in right_rows {
                        let mut found = false;

                        for existing_ctx in &joined_rows {
                            let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = existing_ctx
                                .iter()
                                .map(|(t, a, r)| (*t, a.as_deref(), r))
                                .chain(std::iter::once((
                                    join_table,
                                    join_alias.as_deref(),
                                    right_row,
                                )))
                                .collect();
                            let eval_ctx =
                                EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state);

                            if evaluate_condition_joined(self, &join.on, &eval_ctx)? {
                                found = true;
                                let mut new_ctx = existing_ctx.clone();
                                new_ctx.push((join_table, join_alias.clone(), right_row.clone()));
                                matching_rows.push(new_ctx);
                            }
                        }

                        if !found {
                            let mut null_ctx = Vec::new();
                            for (lt, la, _lr) in &joined_rows[0] {
                                null_ctx.push((*lt, (*la).clone(), lt.null_row()));
                            }
                            null_ctx.push((join_table, join_alias.clone(), right_row.clone()));
                            matching_rows.push(null_ctx);
                        }
                    }

                    joined_rows = matching_rows;
                }
                _ => {
                    let mut next_joined_rows = Vec::new();

                    for existing_ctx in joined_rows {
                        let mut found_match = false;
                        for new_row in &join_table.data.rows {
                            let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = existing_ctx
                                .iter()
                                .map(|(t, a, r)| (*t, a.as_deref(), r))
                                .chain(std::iter::once((
                                    join_table,
                                    join_alias.as_deref(),
                                    new_row,
                                )))
                                .collect();

                            let eval_ctx =
                                EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state);

                            if evaluate_condition_joined(self, &join.on, &eval_ctx)? {
                                let mut next_ctx = existing_ctx.clone();
                                next_ctx.push((join_table, join_alias.clone(), new_row.clone()));
                                next_joined_rows.push(next_ctx);
                                found_match = true;
                            }
                        }

                        if !found_match && join.join_type == squeal::ir::JoinType::Left {
                            let mut next_ctx = existing_ctx.clone();
                            next_ctx.push((join_table, join_alias.clone(), join_table.null_row()));
                            next_joined_rows.push(next_ctx);
                        }
                    }
                    joined_rows = next_joined_rows;
                }
            }
        }
        Ok(joined_rows)
    }
}
