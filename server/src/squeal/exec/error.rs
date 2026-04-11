use crate::storage::error::StorageError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type ParseResult<T> = std::result::Result<T, ExecError>;

pub type ExecResult<T> = std::result::Result<T, ExecError>;

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum ExecError {
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
    Storage(#[from] StorageError),
}
