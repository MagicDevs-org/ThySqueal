use crate::squeal::exec::ExecResult;
use crate::squeal::exec::{Executor, SelectQueryPlan};
use crate::squeal::ir::WithClause as SquealWithClause;
use crate::storage::{Column, DataType, Row, Table, Value};
use std::collections::HashMap;

impl Executor {
    pub async fn resolve_ctes<'a>(
        &'a self,
        plan: &'a SelectQueryPlan<'a>,
    ) -> ExecResult<HashMap<String, Table>> {
        let stmt = &plan.stmt;
        let outer_contexts = plan.outer_contexts;
        let params = plan.params;
        let db_state = plan.db_state;
        let session = &plan.session;

        let mut cte_tables = HashMap::new();
        if let Some(with) = &stmt.with_clause {
            if with.recursive {
                self.resolve_recursive_ctes(
                    with,
                    db_state,
                    session,
                    outer_contexts,
                    params,
                    &mut cte_tables,
                )
                .await?;
            } else {
                for cte in &with.ctes {
                    let sub_plan =
                        SelectQueryPlan::new(cte.query.clone(), db_state, session.clone())
                            .with_outer_contexts(outer_contexts)
                            .with_params(params)
                            .with_cte_tables(&cte_tables);

                    let res = self.exec_select_recursive(sub_plan).await?;
                    let mut cols = Vec::new();
                    for name in &res.columns {
                        cols.push(Column {
                            name: name.clone(),
                            data_type: DataType::Text,
                            is_auto_increment: false,
                            is_not_null: false,
                            default_value: None,
                        });
                    }
                    let mut table = Table::new(cte.name.clone(), cols, None, vec![]);
                    table.data.rows = res
                        .rows
                        .into_iter()
                        .enumerate()
                        .map(|(i, values)| Row {
                            id: format!("cte_{}_{}", cte.name, i),
                            values,
                        })
                        .collect();
                    cte_tables.insert(cte.name.clone(), table);
                }
            }
        }
        Ok(cte_tables)
    }

    async fn resolve_recursive_ctes<'a>(
        &'a self,
        with_clause: &'a SquealWithClause,
        db_state: &'a crate::storage::DatabaseState,
        session: &'a crate::squeal::exec::Session,
        outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        params: &'a [Value],
        cte_tables: &mut HashMap<String, Table>,
    ) -> ExecResult<()> {
        for cte in &with_clause.ctes {
            let mut all_rows: Vec<Vec<Value>> = Vec::new();
            loop {
                let sub_plan = SelectQueryPlan::new(cte.query.clone(), db_state, session.clone())
                    .with_outer_contexts(outer_contexts)
                    .with_params(params)
                    .with_cte_tables(cte_tables);

                let res = self.exec_select_recursive(sub_plan).await?;

                if res.rows.is_empty() {
                    break;
                }

                let mut new_rows = Vec::new();
                for row in res.rows {
                    if !all_rows.contains(&row) {
                        new_rows.push(row.clone());
                        all_rows.push(row);
                    }
                }

                if new_rows.is_empty() {
                    break;
                }

                let mut cols = Vec::new();
                for name in &res.columns {
                    cols.push(Column {
                        name: name.clone(),
                        data_type: DataType::Text,
                        is_auto_increment: false,
                        is_not_null: false,
                        default_value: None,
                    });
                }

                let mut table = Table::new(cte.name.clone(), cols.clone(), None, vec![]);
                table.data.rows = all_rows
                    .iter()
                    .enumerate()
                    .map(|(i, values)| Row {
                        id: format!("cte_{}_{}", cte.name, i),
                        values: values.clone(),
                    })
                    .collect();
                cte_tables.insert(cte.name.clone(), table);
            }
        }
        Ok(())
    }
}
