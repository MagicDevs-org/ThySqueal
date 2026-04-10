pub mod aggregate;
pub mod ddl;
pub mod dispatch;
pub mod dml;
pub mod dump;
pub mod exec;
pub mod explain;
pub mod helpers;
pub mod kv_hash;
pub mod kv_list;
pub mod kv_set;
pub mod kv_stream;
pub mod kv_string;
pub mod kv_zset;
pub mod materialized;
pub mod plan;
pub mod privilege;
pub mod pubsub;
pub mod result;
pub mod search;
pub mod select;
pub mod session;
pub mod set;
#[cfg(test)]
mod tests;
pub mod tx;
pub mod user;
pub mod window;

pub use plan::SelectQueryPlan;
pub use pubsub::PubSubState;
pub use result::QueryResult;
pub use session::{ExecutionContext, Session};

use crate::engines::mysql::error::SqlResult;
use crate::squeal::ir::{Select, Squeal};
use crate::storage::{Database, DatabaseState, Row, Table, Value};
use dashmap::DashMap;
use futures::future::BoxFuture;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Executor {
    pub(crate) db: Arc<RwLock<Database>>,
    pub(crate) transactions: DashMap<String, DatabaseState>,
    pub(crate) prepared_statements: DashMap<String, Squeal>, // name -> stmt
    pub(crate) data_dir: Option<String>,
    pub(crate) pubsub: Arc<tokio::sync::RwLock<PubSubState>>,
}

impl Executor {
    pub fn new(db: Arc<RwLock<Database>>) -> Self {
        Self {
            db,
            transactions: DashMap::new(),
            prepared_statements: DashMap::new(),
            data_dir: None,
            pubsub: Arc::new(tokio::sync::RwLock::new(PubSubState::default())),
        }
    }

    pub fn with_data_dir(mut self, data_dir: String) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    pub async fn execute(
        &self,
        sql: &str,
        params: Vec<Value>,
        session: Session,
    ) -> SqlResult<QueryResult> {
        // Workflow: SQL string -> AST (Pest) -> Squeal (IR) -> Executor
        let ast = crate::engines::mysql::parser::parse(sql)?;
        let squeal = Squeal::from(ast);
        self.exec_squeal(squeal, params, session).await
    }

    pub async fn execute_squeal(
        &self,
        squeal: Squeal,
        params: Vec<Value>,
        session: Session,
    ) -> SqlResult<QueryResult> {
        self.exec_squeal(squeal, params, session).await
    }
}

impl crate::engines_mysql::eval::Evaluator for Executor {
    fn exec_select_internal<'a>(
        &'a self,
        stmt: Select,
        outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        params: &'a [Value],
        db_state: &'a DatabaseState,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        let plan = SelectQueryPlan::new(stmt, db_state, Session::root())
            .with_outer_contexts(outer_contexts)
            .with_params(params);
        self.exec_select_recursive(plan)
    }
}
