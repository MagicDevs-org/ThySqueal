use super::{Executor, QueryResult};
use crate::squeal::exec::ExecResult;
use crate::squeal::ir::KillStmt;

impl Executor {
    pub async fn exec_kill(&self, stmt: KillStmt) -> ExecResult<QueryResult> {
        tracing::info!(
            "KILL: terminating connection {} (type: {:?})",
            stmt.connection_id,
            stmt.kill_type
        );
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }
}
