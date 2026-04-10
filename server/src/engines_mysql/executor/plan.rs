use super::session::Session;
use crate::squeal::ir::Select;
use crate::storage::{DatabaseState, Row, Table, Value};
use std::collections::HashMap;

pub struct SelectQueryPlan<'a> {
    pub stmt: Select,
    pub outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
    pub params: &'a [Value],
    pub db_state: &'a DatabaseState,
    pub session: Session,
    pub cte_tables: Option<&'a HashMap<String, Table>>,
}

impl<'a> SelectQueryPlan<'a> {
    pub fn new(stmt: Select, db_state: &'a DatabaseState, session: Session) -> Self {
        Self {
            stmt,
            outer_contexts: &[],
            params: &[],
            db_state,
            session,
            cte_tables: None,
        }
    }

    pub fn with_outer_contexts(
        mut self,
        contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
    ) -> Self {
        self.outer_contexts = contexts;
        self
    }

    pub fn with_params(mut self, params: &'a [Value]) -> Self {
        self.params = params;
        self
    }

    pub fn with_cte_tables(mut self, cte_tables: &'a HashMap<String, Table>) -> Self {
        self.cte_tables = Some(cte_tables);
        self
    }
}
