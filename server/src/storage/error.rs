use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error("Row not found: {0}")]
    RowNotFound(String),
    #[error("Column not found: {0}")]
    #[allow(dead_code)]
    ColumnNotFound(String),
    #[error("Duplicate key: {0}")]
    DuplicateKey(String),
    #[error("Invalid type: {0}")]
    InvalidType(String),
    #[error("Persistence error: {0}")]
    PersistenceError(String),
}
