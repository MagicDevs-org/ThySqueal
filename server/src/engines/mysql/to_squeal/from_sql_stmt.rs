use crate::engines::mysql::ast::SqlStmt;
use crate::squeal::ir::Expression;
use crate::squeal::ir::stmt::*;
use crate::storage::Value;

// Conversions from AST to Squeal IR
impl From<SqlStmt> for Squeal {
    fn from(stmt: SqlStmt) -> Self {
        match stmt {
            SqlStmt::CreateTable(s) => Squeal::CreateTable(s.into()),
            SqlStmt::CreateDatabase(s) => Squeal::CreateDatabase(s.into()),
            SqlStmt::DropDatabase(s) => Squeal::DropDatabase(s.into()),
            SqlStmt::CreateTrigger(s) => Squeal::CreateTrigger(s.into()),
            SqlStmt::DropTrigger(s) => Squeal::DropTrigger(s.into()),
            SqlStmt::CreateMaterializedView(s) => Squeal::CreateMaterializedView(s.into()),
            SqlStmt::CreateView(s) => Squeal::CreateView(crate::squeal::ir::stmt::CreateView {
                name: s.name,
                query: s.query.into(),
                columns: s.columns,
                with_check_option: s.with_check_option,
            }),
            SqlStmt::DropView(s) => {
                Squeal::DropView(crate::squeal::ir::stmt::DropView { name: s.name })
            }
            SqlStmt::AlterTable(s) => Squeal::AlterTable(s.into()),
            SqlStmt::DropTable(s) => Squeal::DropTable(s.into()),
            SqlStmt::CreateIndex(s) => Squeal::CreateIndex(s.into()),
            SqlStmt::CreateUser(s) => Squeal::CreateUser(s.into()),
            SqlStmt::DropUser(s) => Squeal::DropUser(s.into()),
            SqlStmt::Grant(s) => Squeal::Grant(s.into()),
            SqlStmt::Revoke(s) => Squeal::Revoke(s.into()),
            SqlStmt::Select(s) => Squeal::Select(s.into()),
            SqlStmt::Insert(s) => Squeal::Insert(s.into()),
            SqlStmt::Update(s) => Squeal::Update(s.into()),
            SqlStmt::Delete(s) => Squeal::Delete(s.into()),
            SqlStmt::Explain(s) => Squeal::Explain(s.into()),
            SqlStmt::Describe(table) => {
                let show_query = format!(
                    "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, EXTRA \
                     FROM information_schema.COLUMNS WHERE TABLE_NAME = '{}' ORDER BY ORDINAL_POSITION",
                    table.replace('\'', "''")
                );
                crate::engines::mysql::parser::parse_to_squeal(&show_query).unwrap()
            }
            SqlStmt::Use(_db) => {
                let select = Select {
                    with_clause: None,
                    columns: vec![SelectColumn {
                        expr: Expression::Literal(Value::Int(1)),
                        alias: None,
                    }],
                    table: String::new(),
                    table_alias: None,
                    distinct: false,
                    joins: vec![],
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                    set_operations: vec![],
                };
                Squeal::Select(select)
            }
            SqlStmt::Search(s) => Squeal::Search(s.into()),
            SqlStmt::Show(s) => Squeal::Show(Show {
                variant: match s.variant {
                    crate::engines::mysql::ast::ShowVariant::Tables(db) => ShowVariant::Tables(db),
                    crate::engines::mysql::ast::ShowVariant::Databases => ShowVariant::Databases,
                    crate::engines::mysql::ast::ShowVariant::Columns(t) => ShowVariant::Columns(t),
                    crate::engines::mysql::ast::ShowVariant::CreateTable(t) => {
                        ShowVariant::CreateTable(t)
                    }
                    crate::engines::mysql::ast::ShowVariant::CreateDatabase(d) => {
                        ShowVariant::CreateDatabase(d)
                    }
                    crate::engines::mysql::ast::ShowVariant::Index(t) => ShowVariant::Index(t),
                    crate::engines::mysql::ast::ShowVariant::Variables(p) => {
                        ShowVariant::Variables(p)
                    }
                    crate::engines::mysql::ast::ShowVariant::Status(p) => ShowVariant::Status(p),
                    crate::engines::mysql::ast::ShowVariant::Processlist => {
                        ShowVariant::Processlist
                    }
                },
            }),
            SqlStmt::Prepare(s) => Squeal::Prepare(s.into()),
            SqlStmt::Execute(s) => Squeal::Execute(s.into()),
            SqlStmt::Deallocate(s) => Squeal::Deallocate(s),
            SqlStmt::Set(s) => Squeal::Set(s.into()),
            SqlStmt::Kill(k) => Squeal::Kill(crate::squeal::ir::stmt::KillStmt {
                connection_id: k.connection_id,
                kill_type: match k.kill_type {
                    crate::engines::mysql::ast::KillType::Connection => {
                        crate::squeal::ir::stmt::KillType::Connection
                    }
                    crate::engines::mysql::ast::KillType::Query => {
                        crate::squeal::ir::stmt::KillType::Query
                    }
                },
            }),
            SqlStmt::Begin => Squeal::Begin,
            SqlStmt::Commit => Squeal::Commit,
            SqlStmt::Rollback => Squeal::Rollback,
            SqlStmt::Savepoint(sp) => Squeal::Savepoint(sp),
        }
    }
}
