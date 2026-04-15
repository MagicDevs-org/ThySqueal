use crate::engines::mysql::parser::parse_to_squeal;
use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::Squeal;

use super::privilege::check_privilege;
use super::{ExecutionContext, Executor, QueryResult, SelectQueryPlan, Session};
use crate::storage::{Privilege, Value};
use futures::future::{BoxFuture, FutureExt};

impl Executor {
    pub fn exec_squeal<'a>(
        &'a self,
        stmt: Squeal,
        params: Vec<Value>,
        session: Session,
    ) -> BoxFuture<'a, ExecResult<QueryResult>> {
        async move {
            let ctx = ExecutionContext::new(params, session);

            let mut res = match stmt {
                // Transaction control
                Squeal::Begin | Squeal::Commit | Squeal::Rollback | Squeal::Savepoint(_) => {
                    self.dispatch_tx(stmt, &ctx).await?
                }

                // DDL (Data Definition)
                Squeal::CreateTable(_)
                | Squeal::DropTable(_)
                | Squeal::AlterTable(_)
                | Squeal::CreateIndex(_)
                | Squeal::CreateMaterializedView(_)
                | Squeal::CreateDatabase(_)
                | Squeal::DropDatabase(_)
                | Squeal::CreateTrigger(_)
                | Squeal::DropTrigger(_) => self.dispatch_ddl(stmt, &ctx).await?,

                // DML (Data Manipulation)
                Squeal::Insert(_) | Squeal::Update(_) | Squeal::Delete(_) => {
                    self.dispatch_dml(stmt, &ctx).await?
                }

                // User management
                Squeal::CreateUser(_)
                | Squeal::DropUser(_)
                | Squeal::Grant(_)
                | Squeal::Revoke(_) => self.dispatch_user(stmt, &ctx).await?,

                // Session management
                Squeal::Set(s) => self.exec_set(s, &ctx).await?,
                Squeal::Kill(k) => self.exec_kill(k).await?,

                // Queries
                Squeal::Select(_) | Squeal::Search(_) | Squeal::Explain(_) => {
                    self.dispatch_query(stmt, &ctx).await?
                }

                // Prepared statements
                Squeal::Prepare(p) => self.exec_prepare(p).await?,
                Squeal::Execute(e) => {
                    self.exec_execute(e, ctx.params.clone(), ctx.session.clone())
                        .await?
                }
                Squeal::Deallocate(name) => self.exec_deallocate(&name).await?,

                // KV Store operations
                Squeal::KvSet(kv) => {
                    self.exec_kv_set(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvGet(kv) => {
                    self.exec_kv_get(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvDel(kv) => {
                    self.exec_kv_del(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvHashSet(kv) => {
                    self.exec_kv_hash_set(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvHashGet(kv) => {
                    self.exec_kv_hash_get(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvHashGetAll(kv) => {
                    self.exec_kv_hash_get_all(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvListPush(kv) => {
                    self.exec_kv_list_push(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvListRange(kv) => {
                    self.exec_kv_list_range(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvSetAdd(kv) => {
                    self.exec_kv_set_add(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvSetMembers(kv) => {
                    self.exec_kv_set_members(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvZSetAdd(kv) => {
                    self.exec_kv_zset_add(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvZSetRange(kv) => {
                    self.exec_kv_zset_range(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvStreamAdd(kv) => {
                    self.exec_kv_stream_add(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvStreamRange(kv) => {
                    self.exec_kv_stream_range(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::KvStreamLen(kv) => {
                    self.exec_kv_stream_len(kv, ctx.session.transaction_id.as_deref())
                        .await?
                }
                Squeal::PubSubPublish(kv) => self.exec_pubsub_publish(kv).await?,
            };

            if res.transaction_id.is_none() {
                res.transaction_id = ctx.session.transaction_id.clone();
            }
            if res.session.is_none() {
                res.session = Some(ctx.session);
            }

            Ok(res)
        }
        .boxed()
    }

    async fn dispatch_tx(&self, stmt: Squeal, ctx: &ExecutionContext) -> ExecResult<QueryResult> {
        match stmt {
            Squeal::Begin => self.exec_begin().await,
            Squeal::Commit => {
                self.exec_commit(ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::Rollback => {
                self.exec_rollback(ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::Savepoint(sp) => {
                self.exec_savepoint(&sp.name, ctx.session.transaction_id.as_deref())
                    .await
            }
            _ => unreachable!(),
        }
    }

    async fn dispatch_ddl(&self, stmt: Squeal, ctx: &ExecutionContext) -> ExecResult<QueryResult> {
        match stmt {
            Squeal::CreateTable(ct) => {
                {
                    let db = self.db.read().await;
                    check_privilege(&ctx.session.username, None, Privilege::Create, db.state())?;
                }
                self.exec_create_table(ct, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::CreateMaterializedView(mv) => {
                {
                    let db = self.db.read().await;
                    check_privilege(&ctx.session.username, None, Privilege::Create, db.state())?;
                }
                self.exec_create_materialized_view(mv, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::AlterTable(at) => {
                {
                    let db = self.db.read().await;
                    check_privilege(
                        &ctx.session.username,
                        Some(&at.table),
                        Privilege::Create,
                        db.state(),
                    )?;
                }
                self.exec_alter_table(at, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::DropTable(dt) => {
                {
                    let db = self.db.read().await;
                    check_privilege(
                        &ctx.session.username,
                        Some(&dt.name),
                        Privilege::Drop,
                        db.state(),
                    )?;
                }
                self.exec_drop_table(dt, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::CreateIndex(ci) => {
                {
                    let db = self.db.read().await;
                    check_privilege(
                        &ctx.session.username,
                        Some(&ci.table),
                        Privilege::Create,
                        db.state(),
                    )?;
                }
                self.exec_create_index(ci, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::CreateDatabase(cd) => {
                {
                    let db = self.db.read().await;
                    check_privilege(&ctx.session.username, None, Privilege::Create, db.state())?;
                }
                self.exec_create_database(cd, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::DropDatabase(dd) => {
                {
                    let db = self.db.read().await;
                    check_privilege(&ctx.session.username, None, Privilege::Drop, db.state())?;
                }
                self.exec_drop_database(dd, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::CreateTrigger(ct) => {
                {
                    let db = self.db.read().await;
                    check_privilege(&ctx.session.username, None, Privilege::Create, db.state())?;
                }
                self.exec_create_trigger(ct, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::DropTrigger(dt) => {
                {
                    let db = self.db.read().await;
                    check_privilege(&ctx.session.username, None, Privilege::Drop, db.state())?;
                }
                self.exec_drop_trigger(dt, ctx.session.transaction_id.as_deref())
                    .await
            }
            _ => unreachable!(),
        }
    }

    async fn dispatch_dml(&self, stmt: Squeal, ctx: &ExecutionContext) -> ExecResult<QueryResult> {
        match stmt {
            Squeal::Insert(i) => {
                {
                    let db = self.db.read().await;
                    check_privilege(
                        &ctx.session.username,
                        Some(&i.table),
                        Privilege::Insert,
                        db.state(),
                    )?;
                }
                self.exec_insert(i, &ctx.params, ctx.session.clone()).await
            }
            Squeal::Update(u) => {
                {
                    let db = self.db.read().await;
                    check_privilege(
                        &ctx.session.username,
                        Some(&u.table),
                        Privilege::Update,
                        db.state(),
                    )?;
                }
                self.exec_update(u, &ctx.params, ctx.session.clone()).await
            }
            Squeal::Delete(d) => {
                {
                    let db = self.db.read().await;
                    check_privilege(
                        &ctx.session.username,
                        Some(&d.table),
                        Privilege::Delete,
                        db.state(),
                    )?;
                }
                self.exec_delete(d, &ctx.params, ctx.session.clone()).await
            }
            _ => unreachable!(),
        }
    }

    async fn dispatch_user(&self, stmt: Squeal, ctx: &ExecutionContext) -> ExecResult<QueryResult> {
        {
            let db = self.db.read().await;
            check_privilege(&ctx.session.username, None, Privilege::Grant, db.state())?;
        }
        match stmt {
            Squeal::CreateUser(cu) => {
                self.exec_create_user(cu, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::DropUser(du) => {
                self.exec_drop_user(du, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::Grant(g) => {
                self.exec_grant(g, ctx.session.transaction_id.as_deref())
                    .await
            }
            Squeal::Revoke(r) => {
                self.exec_revoke(r, ctx.session.transaction_id.as_deref())
                    .await
            }
            _ => unreachable!(),
        }
    }

    async fn dispatch_query(
        &self,
        stmt: Squeal,
        ctx: &ExecutionContext,
    ) -> ExecResult<QueryResult> {
        match stmt {
            Squeal::Select(s) => {
                let table = s.table.clone();
                if let Some(id) = &ctx.session.transaction_id {
                    let state = self
                        .transactions
                        .get(id)
                        .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?;
                    if !table.is_empty() && !table.starts_with("information_schema.") {
                        check_privilege(
                            &ctx.session.username,
                            Some(&table),
                            Privilege::Select,
                            &state,
                        )?;
                    }

                    let plan = SelectQueryPlan::new(s, &state, ctx.session.clone())
                        .with_params(&ctx.params);

                    self.exec_select_recursive(plan).await
                } else {
                    let db = self.db.read().await;
                    if !table.is_empty() && !table.starts_with("information_schema.") {
                        check_privilege(
                            &ctx.session.username,
                            Some(&table),
                            Privilege::Select,
                            db.state(),
                        )?;
                    }

                    let plan = SelectQueryPlan::new(s, db.state(), ctx.session.clone())
                        .with_params(&ctx.params);

                    self.exec_select_recursive(plan).await
                }
            }
            Squeal::Search(s) => {
                if let Some(id) = &ctx.session.transaction_id {
                    let state = self
                        .transactions
                        .get(id)
                        .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?;
                    check_privilege(
                        &ctx.session.username,
                        Some(&s.table),
                        Privilege::Select,
                        &state,
                    )?;
                    self.exec_search(s, &state, Some(id)).await
                } else {
                    let db = self.db.read().await;
                    check_privilege(
                        &ctx.session.username,
                        Some(&s.table),
                        Privilege::Select,
                        db.state(),
                    )?;
                    self.exec_search(s, db.state(), None).await
                }
            }
            Squeal::Explain(s) => {
                if let Some(id) = &ctx.session.transaction_id {
                    let state = self
                        .transactions
                        .get(id)
                        .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?;
                    self.exec_explain(s, &state, Some(id)).await
                } else {
                    let db = self.db.read().await;
                    self.exec_explain(s, db.state(), None).await
                }
            }
            _ => unreachable!(),
        }
    }

    pub async fn exec_prepare(&self, stmt: crate::squeal::ir::Prepare) -> ExecResult<QueryResult> {
        let squeal = parse_to_squeal(&stmt.sql)?;
        self.prepared_statements.insert(stmt.name, squeal);
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }

    pub async fn exec_execute(
        &self,
        stmt: crate::squeal::ir::Execute,
        params: Vec<Value>,
        session: Session,
    ) -> ExecResult<QueryResult> {
        let prepared = self.prepared_statements.get(&stmt.name).ok_or_else(|| {
            ExecError::Runtime(format!("Prepared statement '{}' not found", stmt.name))
        })?;
        let inner_stmt = prepared.value().clone();
        drop(prepared); // Release lock before recursive call

        let mut exec_params = Vec::new();
        if !stmt.params.is_empty() {
            for p in &stmt.params {
                let db = self.db.read().await;
                let state = if let Some(id) = &session.transaction_id {
                    self.transactions
                        .get(id)
                        .ok_or_else(|| ExecError::Runtime("Transaction not found".to_string()))?
                        .clone()
                } else {
                    db.state().clone()
                };

                let eval_ctx = crate::squeal::eval::EvalContext::new(&[], &params, &[], &state)
                    .with_session(&session);
                let val = crate::squeal::eval::evaluate_expression_joined(self, p, &eval_ctx)?;
                exec_params.push(val);
            }
        } else {
            exec_params = params;
        }

        self.exec_squeal(inner_stmt, exec_params, session).await
    }

    pub async fn exec_deallocate(&self, name: &str) -> ExecResult<QueryResult> {
        self.prepared_statements.remove(name);
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
            session: None,
        })
    }
}
