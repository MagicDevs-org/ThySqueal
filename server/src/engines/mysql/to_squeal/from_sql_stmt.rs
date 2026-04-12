use crate::engines::mysql::ast::SqlStmt;
use crate::squeal::ir::stmt::*;

// Conversions from AST to Squeal IR
impl From<SqlStmt> for Squeal {
    fn from(stmt: SqlStmt) -> Self {
        match stmt {
            SqlStmt::CreateTable(s) => Squeal::CreateTable(s.into()),
            SqlStmt::CreateMaterializedView(s) => Squeal::CreateMaterializedView(s.into()),
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
            SqlStmt::Search(s) => Squeal::Search(s.into()),
            SqlStmt::Prepare(s) => Squeal::Prepare(s.into()),
            SqlStmt::Execute(s) => Squeal::Execute(s.into()),
            SqlStmt::Deallocate(s) => Squeal::Deallocate(s),
            SqlStmt::Set(s) => Squeal::Set(s.into()),
            SqlStmt::Begin => Squeal::Begin,
            SqlStmt::Commit => Squeal::Commit,
            SqlStmt::Rollback => Squeal::Rollback,
            SqlStmt::Savepoint(sp) => Squeal::Savepoint(sp),
        }
    }
}
