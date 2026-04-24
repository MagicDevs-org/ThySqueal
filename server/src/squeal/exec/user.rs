use super::{Executor, QueryResult};
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::{CreateUser, DropUser, Grant, Revoke};
use crate::storage::User;
use sha1::{Digest, Sha1};
use std::collections::HashMap;

fn compute_sha1_hash(password: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    hex::encode(hasher.finalize())
}

impl Executor {
    pub async fn exec_create_user(
        &self,
        stmt: CreateUser,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        let hashed = bcrypt::hash(&stmt.password, bcrypt::DEFAULT_COST)
            .map_err(|e| ExecError::Runtime(format!("Bcrypt error: {}", e)))?;
        let auth_string = compute_sha1_hash(&stmt.password);

        self.mutate_state(tx_id, |state| {
            if state.users.contains_key(&stmt.username) {
                return Err(ExecError::Runtime(format!(
                    "User {} already exists",
                    stmt.username
                )));
            }
            state.users.insert(
                stmt.username.clone(),
                User {
                    username: stmt.username,
                    password_hash: hashed,
                    auth_string: Some(auth_string),
                    global_privileges: vec![],
                    table_privileges: HashMap::new(),
                },
            );
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
            session: None,
        })
    }

    pub async fn exec_drop_user(
        &self,
        stmt: DropUser,
        tx_id: Option<&str>,
    ) -> ExecResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            state
                .users
                .remove(&stmt.username)
                .ok_or_else(|| ExecError::Runtime(format!("User {} not found", stmt.username)))?;
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
            session: None,
        })
    }

    pub async fn exec_grant(&self, stmt: Grant, tx_id: Option<&str>) -> ExecResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            let user = state
                .users
                .get_mut(&stmt.username)
                .ok_or_else(|| ExecError::Runtime(format!("User {} not found", stmt.username)))?;

            if let Some(table) = &stmt.table {
                let entry = user.table_privileges.entry(table.clone()).or_default();
                for p in &stmt.privileges {
                    if !entry.contains(p) {
                        entry.push(p.clone());
                    }
                }
            } else {
                for p in &stmt.privileges {
                    if !user.global_privileges.contains(p) {
                        user.global_privileges.push(p.clone());
                    }
                }
            }
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
            session: None,
        })
    }

    pub async fn exec_revoke(&self, stmt: Revoke, tx_id: Option<&str>) -> ExecResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            let user = state
                .users
                .get_mut(&stmt.username)
                .ok_or_else(|| ExecError::Runtime(format!("User {} not found", stmt.username)))?;

            if let Some(table) = &stmt.table {
                if let Some(entry) = user.table_privileges.get_mut(table) {
                    entry.retain(|p| !stmt.privileges.contains(p));
                }
            } else {
                user.global_privileges
                    .retain(|p| !stmt.privileges.contains(p));
            }
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
            session: None,
        })
    }
}
