use crate::engines::mysql::error::SqlResult;
use crate::engines_mysql::executor::{Executor, SelectQueryPlan, Session};
use crate::storage::{DatabaseState, Row};

impl Executor {
    pub fn refresh_materialized_views(&self, state: &mut DatabaseState) -> SqlResult<()> {
        let views = state.materialized_views.clone();
        for (name, query) in views {
            let plan = SelectQueryPlan::new(query, state, Session::root());
            let res = futures::executor::block_on(self.exec_select_recursive(plan))?;

            if let Some(table) = state.tables.get_mut(&name) {
                table.data.rows = res
                    .rows
                    .into_iter()
                    .enumerate()
                    .map(|(i, values)| Row {
                        id: format!("mv_{}_{}", name, i),
                        values,
                    })
                    .collect();
            }
        }
        Ok(())
    }
}
