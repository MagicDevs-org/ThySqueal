use super::cond::Condition;
use super::expr::Expression;
use crate::storage::{Column, DataType, ForeignKey, Privilege, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Squeal Internal Representation (IR) of a query.
/// This layer decouples the execution engine from the parser AST.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Squeal {
    CreateTable(CreateTable),
    CreateDatabase(CreateDatabase),
    DropDatabase(DropDatabase),
    CreateTrigger(CreateTrigger),
    DropTrigger(DropTrigger),
    CreateMaterializedView(CreateMaterializedView),
    AlterTable(AlterTable),
    DropTable(DropTable),
    CreateIndex(CreateIndex),
    CreateUser(CreateUser),
    DropUser(DropUser),
    Grant(Grant),
    Revoke(Revoke),
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Explain(Select),
    Search(Search),
    Prepare(Prepare),
    Execute(Execute),
    Deallocate(String),
    Set(Set),
    Kill(KillStmt),
    Begin,
    Commit,
    Rollback,
    Savepoint(SavepointStmt),
    KvSet(KvSet),
    KvGet(KvGet),
    KvDel(KvDel),
    KvHashSet(KvHashSet),
    KvHashGet(KvHashGet),
    KvHashGetAll(KvHashGetAll),
    KvListPush(KvListPush),
    KvListRange(KvListRange),
    KvSetAdd(KvSetAdd),
    KvSetMembers(KvSetMembers),
    KvZSetAdd(KvZSetAdd),
    KvZSetRange(KvZSetRange),
    KvStreamAdd(KvStreamAdd),
    KvStreamRange(KvStreamRange),
    KvStreamLen(KvStreamLen),
    PubSubPublish(PubSubPublish),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvSet {
    pub key: String,
    pub value: Value,
    pub expiry: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvGet {
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvDel {
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvHashSet {
    pub key: String,
    pub field: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvHashGet {
    pub key: String,
    pub field: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvHashGetAll {
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvListPush {
    pub key: String,
    pub values: Vec<Value>,
    pub left: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvListRange {
    pub key: String,
    pub start: i64,
    pub stop: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvSetAdd {
    pub key: String,
    pub members: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvSetMembers {
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvZSetAdd {
    pub key: String,
    pub members: Vec<(f64, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvZSetRange {
    pub key: String,
    pub start: i64,
    pub stop: i64,
    pub with_scores: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvStreamAdd {
    pub key: String,
    pub id: Option<u64>,
    pub fields: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvStreamRange {
    pub key: String,
    pub start: String,
    pub stop: String,
    pub count: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvStreamLen {
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PubSubPublish {
    pub channel: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Set {
    pub assignments: Vec<(Expression, Expression)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Select {
    pub with_clause: Option<WithClause>,
    pub columns: Vec<SelectColumn>,
    pub table: String,
    pub table_alias: Option<String>,
    pub distinct: bool,
    pub joins: Vec<Join>,
    pub where_clause: Option<Condition>,
    pub group_by: Vec<Expression>,
    pub having: Option<Condition>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<LimitClause>,
    pub set_operations: Vec<SetOperationClause>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOperator {
    Union,
    UnionAll,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetOperationClause {
    pub operator: SetOperator,
    pub select: Box<Select>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WithClause {
    pub recursive: bool,
    pub ctes: Vec<Cte>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cte {
    pub name: String,
    pub query: Select,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Join {
    pub table: String,
    pub table_alias: Option<String>,
    pub join_type: JoinType,
    pub on: Condition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectColumn {
    pub expr: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByItem {
    pub expr: Expression,
    pub order: Order,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Order {
    Asc,
    Desc,
}

impl Order {
    pub fn is_asc(&self) -> bool {
        *self == Order::Asc
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitClause {
    pub count: usize,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InsertMode {
    Normal,
    Replace,
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Insert {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Expression>,
    pub mode: InsertMode,
    pub on_duplicate_update: Option<Vec<(String, Expression)>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Condition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub table: String,
    pub where_clause: Option<Condition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SavepointStmt {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KillStmt {
    pub connection_id: u64,
    pub kill_type: KillType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KillType {
    Connection,
    Query,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateMaterializedView {
    pub name: String,
    pub query: Select,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterTable {
    pub table: String,
    pub action: AlterAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterAction {
    AddColumn(Column),
    DropColumn(String),
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    RenameTable(String),
    ModifyColumn {
        name: String,
        data_type: DataType,
    },
    SetDefault {
        column: String,
        value: Option<Value>,
    },
    DropDefault {
        column: String,
    },
    SetNotNull {
        column: String,
    },
    DropNotNull {
        column: String,
    },
    AddPrimaryKey {
        columns: Vec<String>,
    },
    DropPrimaryKey,
    AddForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
    DropForeignKey {
        name: String,
    },
    AlterEngine {
        engine: String,
    },
    AlterCharset {
        charset: String,
        collation: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateIndex {
    pub name: String,
    pub table: String,
    pub expressions: Vec<Expression>,
    pub unique: bool,
    pub index_type: IndexType,
    pub where_clause: Option<Condition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTable {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateDatabase {
    pub name: String,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropDatabase {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTrigger {
    pub name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub table: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTrigger {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateUser {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropUser {
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grant {
    pub privileges: Vec<Privilege>,
    pub table: Option<String>,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Revoke {
    pub privileges: Vec<Privilege>,
    pub table: Option<String>,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Search {
    pub table: String,
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Prepare {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Execute {
    pub name: String,
    pub params: Vec<Expression>,
}
