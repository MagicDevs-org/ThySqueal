use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataType {
    TinyInt,
    TinyUInt,
    SmallInt,
    SmallUInt,
    Int,
    UInt,
    BigInt,
    BigUInt,
    Decimal(usize, usize),
    Float,
    Double,
    Bool,
    Date,
    DateTime,
    Time,
    TimeStamp,
    VarChar(Option<usize>),
    Char(usize),
    Text,
    Blob,
    Binary(Option<usize>),
    VarBinary(Option<usize>),
    Enum(Vec<String>),
    Set(Vec<String>),
    Json,
}

impl DataType {
    pub fn from_str(s: &str) -> Self {
        let upper = s.to_uppercase();
        if let Some(paren_idx) = upper.find('(') {
            let name = upper[..paren_idx].trim();
            let rest = upper[paren_idx..].trim_matches(|c| c == '(' || c == ')');
            match name {
                "DECIMAL" | "NUMERIC" => {
                    let parts: Vec<&str> = rest.split(',').collect();
                    let (p, s) = if parts.len() == 2 {
                        (
                            parts[0].parse().unwrap_or(10),
                            parts[1].parse().unwrap_or(0),
                        )
                    } else {
                        (10, 0)
                    };
                    return DataType::Decimal(p, s);
                }
                "CHAR" | "CHARACTER" => {
                    let len: usize = rest.parse().unwrap_or(1);
                    return DataType::Char(len);
                }
                "VARCHAR" | "CHARACTER VARYING" => {
                    let len: usize = rest.parse().unwrap_or(255);
                    return DataType::VarChar(Some(len));
                }
                "BINARY" => {
                    let len: usize = rest.parse().unwrap_or(1);
                    return DataType::Binary(Some(len));
                }
                "VARBINARY" => {
                    let len: usize = rest.parse().unwrap_or(255);
                    return DataType::VarBinary(Some(len));
                }
                "ENUM" => {
                    let values: Vec<String> = rest
                        .split(',')
                        .map(|v| v.trim().trim_matches('\'').to_string())
                        .collect();
                    return DataType::Enum(values);
                }
                "SET" => {
                    let values: Vec<String> = rest
                        .split(',')
                        .map(|v| v.trim().trim_matches('\'').to_string())
                        .collect();
                    return DataType::Set(values);
                }
                _ => {}
            }
        }
        match upper.as_str() {
            "TINYINT" => DataType::TinyInt,
            "TINYINT UNSIGNED" => DataType::TinyUInt,
            "SMALLINT" => DataType::SmallInt,
            "SMALLINT UNSIGNED" => DataType::SmallUInt,
            "INT" | "INTEGER" => DataType::Int,
            "INT UNSIGNED" | "INTEGER UNSIGNED" => DataType::UInt,
            "BIGINT" => DataType::BigInt,
            "BIGINT UNSIGNED" => DataType::BigUInt,
            "FLOAT" | "REAL" => DataType::Float,
            "DOUBLE" | "DOUBLE PRECISION" => DataType::Double,
            "BOOL" | "BOOLEAN" => DataType::Bool,
            "DATE" => DataType::Date,
            "DATETIME" => DataType::DateTime,
            "TIME" => DataType::Time,
            "TIMESTAMP" => DataType::TimeStamp,
            "TEXT" => DataType::Text,
            "BLOB" => DataType::Blob,
            "JSON" | "JSONB" => DataType::Json,
            _ => DataType::Text,
        }
    }

    pub fn is_unsigned(&self) -> bool {
        matches!(
            self,
            DataType::TinyUInt | DataType::SmallUInt | DataType::UInt | DataType::BigUInt
        )
    }

    pub fn signed_variant(&self) -> Self {
        match self {
            DataType::TinyUInt => DataType::TinyInt,
            DataType::SmallUInt => DataType::SmallInt,
            DataType::UInt => DataType::Int,
            DataType::BigUInt => DataType::BigInt,
            other => other.clone(),
        }
    }

    pub fn unsigned_variant(&self) -> Self {
        match self {
            DataType::TinyInt => DataType::TinyUInt,
            DataType::SmallInt => DataType::SmallUInt,
            DataType::Int => DataType::UInt,
            DataType::BigInt => DataType::BigUInt,
            other => other.clone(),
        }
    }

    #[allow(dead_code)]
    pub fn to_sql(&self) -> String {
        match self {
            DataType::TinyInt => "TINYINT".to_string(),
            DataType::TinyUInt => "TINYINT UNSIGNED".to_string(),
            DataType::SmallInt => "SMALLINT".to_string(),
            DataType::SmallUInt => "SMALLINT UNSIGNED".to_string(),
            DataType::Int => "INT".to_string(),
            DataType::UInt => "INT UNSIGNED".to_string(),
            DataType::BigInt => "BIGINT".to_string(),
            DataType::BigUInt => "BIGINT UNSIGNED".to_string(),
            DataType::Decimal(p, s) => format!("DECIMAL({}, {})", p, s),
            DataType::Float => "FLOAT".to_string(),
            DataType::Double => "DOUBLE".to_string(),
            DataType::Bool => "BOOL".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::DateTime => "DATETIME".to_string(),
            DataType::Time => "TIME".to_string(),
            DataType::TimeStamp => "TIMESTAMP".to_string(),
            DataType::VarChar(Some(n)) => format!("VARCHAR({})", n),
            DataType::VarChar(None) => "VARCHAR".to_string(),
            DataType::Char(n) => format!("CHAR({})", n),
            DataType::Text => "TEXT".to_string(),
            DataType::Blob => "BLOB".to_string(),
            DataType::Binary(Some(n)) => format!("BINARY({})", n),
            DataType::Binary(None) => "BINARY".to_string(),
            DataType::VarBinary(Some(n)) => format!("VARBINARY({})", n),
            DataType::VarBinary(None) => "VARBINARY".to_string(),
            DataType::Enum(vals) => {
                format!(
                    "ENUM({})",
                    vals.iter()
                        .map(|v| format!("'{}'", v))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DataType::Set(vals) => {
                format!(
                    "SET({})",
                    vals.iter()
                        .map(|v| format!("'{}'", v))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DataType::Json => "JSON".to_string(),
        }
    }
}
