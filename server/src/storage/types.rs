use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataType {
    Int,
    Float,
    Bool,
    Date,
    DateTime,
    VarChar,
    Text,
    Blob,
    Json,
}

impl DataType {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "INT" | "INTEGER" => DataType::Int,
            "FLOAT" | "DOUBLE" | "REAL" => DataType::Float,
            "BOOL" | "BOOLEAN" => DataType::Bool,
            "DATE" => DataType::Date,
            "DATETIME" => DataType::DateTime,
            "VARCHAR" | "TEXT" | "STRING" => DataType::Text,
            "BLOB" | "BINARY" => DataType::Blob,
            "JSON" | "JSONB" => DataType::Json,
            _ => DataType::Text,
        }
    }
}
