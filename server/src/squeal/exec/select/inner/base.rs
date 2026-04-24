use crate::squeal;
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::exec::{Executor, SelectQueryPlan, Session};
use crate::storage::info_schema::get_info_schema_tables;
use crate::storage::{Column, DataType, Row, Table, TableData, TableIndexes, TableSchema, Value};
use std::collections::HashMap;

pub enum ResolvedTable<'b> {
    Physical(&'b Table),
    Cte(&'b Table),
    Virtual(Box<Table>),
}

impl<'b> ResolvedTable<'b> {
    pub fn table(&self) -> &Table {
        match self {
            ResolvedTable::Physical(t) => t,
            ResolvedTable::Cte(t) => t,
            ResolvedTable::Virtual(t) => t,
        }
    }
}

impl Executor {
    pub fn resolve_base_table<'b>(
        &self,
        plan: &SelectQueryPlan<'b>,
        cte_tables: &'b HashMap<String, Table>,
    ) -> ExecResult<(ResolvedTable<'b>, Vec<Row>)> {
        let stmt = &plan.stmt;
        let db_state = plan.db_state;

        if stmt.table.is_empty() {
            let dual_table = Table::new("dual".to_string(), vec![], None, vec![]);
            let rows = vec![Row {
                id: "dual".to_string(),
                values: vec![],
            }];
            Ok((ResolvedTable::Virtual(Box::new(dual_table)), rows))
        } else if let Some(t) = cte_tables.get(&stmt.table) {
            Ok((ResolvedTable::Cte(t), t.data.rows.clone()))
        } else if let Some(v) = db_state.views.get(&stmt.table) {
            let plan = SelectQueryPlan::new(v.query.clone(), db_state, Session::root());
            let res = futures::executor::block_on(self.exec_select_recursive(plan))?;
            let virtual_table = Table {
                schema: TableSchema {
                    name: stmt.table.clone(),
                    columns: res
                        .columns
                        .iter()
                        .map(|n| Column {
                            name: n.clone(),
                            data_type: DataType::Text,
                            is_auto_increment: false,
                            is_not_null: false,
                            default_value: None,
                        })
                        .collect(),
                    primary_key: None,
                    foreign_keys: vec![],
                },
                data: TableData {
                    rows: res
                        .rows
                        .into_iter()
                        .enumerate()
                        .map(|(i, vals)| Row {
                            id: format!("v_{}_{}", stmt.table, i),
                            values: vals,
                        })
                        .collect(),
                    auto_inc_counters: HashMap::new(),
                },
                indexes: TableIndexes {
                    secondary: HashMap::new(),
                    search: None,
                },
            };
            Ok((
                ResolvedTable::Virtual(Box::new(virtual_table.clone())),
                virtual_table.data.rows.clone(),
            ))
        } else if stmt.table.starts_with("information_schema.") {
            let table_name = stmt.table.strip_prefix("information_schema.").unwrap();
            let info_schema_storage = get_info_schema_tables(db_state);
            let t = info_schema_storage
                .get(table_name)
                .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;
            Ok((
                ResolvedTable::Virtual(Box::new(t.clone())),
                t.data.rows.clone(),
            ))
        } else {
            let t = db_state
                .get_table(&stmt.table)
                .ok_or_else(|| ExecError::TableNotFound(stmt.table.clone()))?;

            let rows = if stmt.joins.is_empty() {
                self.apply_index_optimization(t, stmt)
            } else {
                t.data.rows.clone()
            };
            Ok((ResolvedTable::Physical(t), rows))
        }
    }

    fn apply_index_optimization(&self, t: &Table, stmt: &squeal::ir::Select) -> Vec<Row> {
        let mut best_index = None;
        let mut best_estimated_rows = t.data.rows.len();

        if let Some(squeal::ir::Condition::Comparison(
            left_expr,
            squeal::ir::ComparisonOp::Eq,
            squeal::ir::Expression::Literal(val),
        )) = &stmt.where_clause
        {
            for (idx_name, index) in &t.indexes.secondary {
                let exprs = index.expressions();
                if exprs.len() == 1 && &exprs[0] == left_expr {
                    let key = vec![coerce_index_value(val, t, left_expr, &exprs[0])];
                    let estimated = if let Some(ids) = index.get(&key) {
                        ids.len()
                    } else {
                        0
                    };

                    if estimated < best_estimated_rows {
                        best_estimated_rows = estimated;
                        best_index = Some((idx_name, index, key));
                    }
                }
            }
        }

        let selectivity_threshold = (t.data.rows.len() as f64 * 0.3) as usize;

        if let Some((_name, index, key)) = best_index
            && (best_estimated_rows < selectivity_threshold || t.data.rows.len() < 10)
        {
            if let Some(row_ids) = index.get(&key) {
                t.data
                    .rows
                    .iter()
                    .filter(|r| row_ids.contains(&r.id))
                    .cloned()
                    .collect()
            } else {
                t.data.rows.clone()
            }
        } else {
            t.data.rows.clone()
        }
    }
}

#[allow(clippy::collapsible_if)]
fn coerce_index_value(
    val: &Value,
    t: &Table,
    _left_expr: &squeal::ir::Expression,
    index_expr: &squeal::ir::Expression,
) -> Value {
    if let squeal::ir::Expression::Column(col_name) = index_expr {
        if let Some(col_idx) = t.column_index(col_name) {
            let col_type = &t.columns()[col_idx].data_type;
            #[allow(clippy::collapsible_if)]
            if matches!(col_type, DataType::Int) && matches!(val, Value::Text(_)) {
                if let Value::Text(s) = val
                    && let Ok(i) = s.parse::<i64>()
                {
                    return Value::Int(i);
                }
            }
        }
    }
    val.clone()
}
