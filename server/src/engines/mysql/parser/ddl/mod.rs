pub mod alter;
pub mod create;
pub mod drop;

pub use alter::*;
pub use create::*;
pub use drop::*;

use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::engines::mysql::parser::Rule;
use crate::storage::DataType;

pub fn parse_data_type_from_rule(pair: pest::iterators::Pair<Rule>) -> SqlResult<DataType> {
    let type_str = pair.as_str().to_uppercase();
    match type_str.as_str() {
        "INT" | "INTEGER" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "BIGINT" => Ok(DataType::Int),
        "FLOAT" | "DOUBLE" | "REAL" => Ok(DataType::Float),
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "CHARACTER" => Ok(DataType::Text),
        "BOOLEAN" | "BOOL" => Ok(DataType::Bool),
        "DATE" => Ok(DataType::Date),
        "DATETIME" | "TIMESTAMP" => Ok(DataType::DateTime),
        "BLOB" | "BINARY" | "VARBINARY" => Ok(DataType::Blob),
        "DECIMAL" | "NUMERIC" => Ok(DataType::Decimal(10, 2)),
        _ => Err(SqlError::Parse(format!("Unknown data type: {}", type_str))),
    }
}
