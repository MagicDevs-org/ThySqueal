use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::squeal::exec::ExecError;

pub type SqlResult<T> = std::result::Result<T, SqlError>;

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum SqlError {
    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Duplicate key: {0}")]
    DuplicateKey(String),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Execution error: {0}")]
    Runtime(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Internal storage error: {0}")]
    Storage(#[from] crate::storage::error::StorageError),
}

impl SqlError {
    pub fn mysql_errno(&self) -> u16 {
        match self {
            SqlError::Parse(_) => 1064,
            SqlError::TableNotFound(_) => 1146,
            SqlError::ColumnNotFound(_) => 1054,
            SqlError::DuplicateKey(_) => 1062,
            SqlError::TypeMismatch(_) => 1267,
            SqlError::Runtime(_) => 1105,
            SqlError::PermissionDenied(_) => 1144,
            SqlError::Storage(_) => 1030,
        }
    }

    pub fn mysql_sqlstate(&self) -> &'static str {
        match self {
            SqlError::Parse(_) => "42000",
            SqlError::TableNotFound(_) => "42S02",
            SqlError::ColumnNotFound(_) => "42S22",
            SqlError::DuplicateKey(_) => "23000",
            SqlError::TypeMismatch(_) => "22005",
            SqlError::Runtime(_) => "HY000",
            SqlError::PermissionDenied(_) => "42000",
            SqlError::Storage(_) => "HY000",
        }
    }
}

impl From<SqlError> for ExecError {
    fn from(e: SqlError) -> Self {
        match e {
            SqlError::Parse(val) => ExecError::Parse(val),
            SqlError::TableNotFound(val) => ExecError::TableNotFound(val),
            SqlError::ColumnNotFound(val) => ExecError::ColumnNotFound(val),
            SqlError::DuplicateKey(val) => ExecError::DuplicateKey(val),
            SqlError::TypeMismatch(val) => ExecError::TypeMismatch(val),
            SqlError::Runtime(val) => ExecError::Runtime(val),
            SqlError::PermissionDenied(val) => ExecError::PermissionDenied(val),
            SqlError::Storage(val) => ExecError::Storage(val),
        }
    }
}

impl From<ExecError> for SqlError {
    fn from(e: ExecError) -> Self {
        match e {
            ExecError::Parse(val) => SqlError::Parse(val),
            ExecError::TableNotFound(val) => SqlError::TableNotFound(val),
            ExecError::ColumnNotFound(val) => SqlError::ColumnNotFound(val),
            ExecError::DuplicateKey(val) => SqlError::DuplicateKey(val),
            ExecError::TypeMismatch(val) => SqlError::TypeMismatch(val),
            ExecError::Runtime(val) => SqlError::Runtime(val),
            ExecError::PermissionDenied(val) => SqlError::PermissionDenied(val),
            ExecError::Storage(val) => SqlError::Storage(val),
        }
    }
}
