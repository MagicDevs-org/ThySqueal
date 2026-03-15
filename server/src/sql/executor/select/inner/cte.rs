use crate::sql::error::SqlResult;
use crate::sql::executor::{Executor, SelectQueryPlan};
use crate::storage::{Row, Table};
use std::collections::HashMap;

impl Executor {
    pub(crate) async fn resolve_ctes<'a>(
        &'a self,
        plan: &SelectQueryPlan<'a>,
    ) -> SqlResult<HashMap<String, Table>> {
        let stmt = &plan.stmt;
        let outer_contexts = plan.outer_contexts;
        let params = plan.params;
        let db_state = plan.db_state;
        let session = &plan.session;

        let mut cte_tables = HashMap::new();
        if let Some(with) = &stmt.with_clause {
            for cte in &with.ctes {
                let sub_plan = SelectQueryPlan::new(cte.query.clone(), db_state, session.clone())
                    .with_outer_contexts(outer_contexts)
                    .with_params(params);

                let res = self.exec_select_recursive(sub_plan).await?;
                let mut cols = Vec::new();
                for name in &res.columns {
                    cols.push(crate::storage::Column {
                        name: name.clone(),
                        data_type: crate::storage::DataType::Text,
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
        Ok(cte_tables)
    }
}
