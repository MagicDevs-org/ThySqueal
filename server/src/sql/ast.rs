use crate::storage::{Column, Value};

/// Parsed SQL statement AST.
#[derive(Debug, Clone)]
pub enum SqlStmt {
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
}

#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table: String,
    // set_list, where_clause - not yet implemented
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table: String,
    // where_clause - not yet implemented
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<String>,
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table: String,
    pub values: Vec<Value>,
}
