use crate::storage::Value;

#[allow(dead_code)]
#[derive(Clone)]
pub struct PreparedStatement {
    pub id: u64,
    pub query: String,
    pub columns: Vec<ColumnMeta>,
    pub params: Vec<ColumnMeta>,
}

#[derive(Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub type_code: u8,
    pub flags: u16,
}

pub fn parse_parameter_columns(query: &str) -> Vec<ColumnMeta> {
    let mut columns = Vec::new();
    let placeholders: Vec<(usize, &str)> =
        query.match_indices('?').map(|(i, _)| (i, "?")).collect();

    for (idx, _) in placeholders {
        columns.push(ColumnMeta {
            name: format!("@param{}", idx),
            type_code: super::constants::MYSQL_VAR_STRING,
            flags: 0,
        });
    }

    columns
}

pub fn value_to_mysql_type(value: &Value) -> u8 {
    #[allow(unused_imports)]
    use crate::storage::DataType;
    match value {
        Value::Null => super::constants::MYSQL_NULL_TYPE,
        Value::Int(_) => super::constants::MYSQL_INT,
        Value::Float(_) => super::constants::MYSQL_DOUBLE,
        Value::Text(_) => super::constants::MYSQL_VAR_STRING,
        Value::Bool(_) => super::constants::MYSQL_TINYINT,
        Value::DateTime(_) => super::constants::MYSQL_DATETIME,
        Value::Json(_) => super::constants::MYSQL_BLOB,
    }
}

impl Value {
    pub fn to_mysql_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => vec![],
            Value::Int(i) => i.to_le_bytes().to_vec(),
            Value::Float(f) => f.to_le_bytes().to_vec(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Bool(b) => vec![if *b { 1 } else { 0 }],
            Value::DateTime(dt) => dt.to_rfc3339().as_bytes().to_vec(),
            Value::Json(j) => j.to_string().as_bytes().to_vec(),
        }
    }
}

pub fn extract_bound_params(
    data: &[u8],
    param_types: &[ColumnMeta],
    null_bitmap: &[u8],
) -> Vec<Value> {
    let mut params = Vec::new();
    for (i, param) in param_types.iter().enumerate() {
        let null_byte_idx = i / 8;
        let null_bit_idx = i % 8;
        let is_null = null_bitmap
            .get(null_byte_idx)
            .map(|b| (b >> null_bit_idx) & 1 != 0)
            .unwrap_or(false);

        if is_null {
            params.push(Value::Null);
            continue;
        }

        let (value, _bytes_read): (Value, usize) = match param.type_code {
            super::constants::MYSQL_INT
            | super::constants::MYSQL_TINYINT
            | super::constants::MYSQL_SMALLINT => {
                if data.len() >= 4 {
                    let val = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                    (Value::Int(val as i64), 4)
                } else {
                    (Value::Null, 0)
                }
            }
            super::constants::MYSQL_BIGINT => {
                if data.len() >= 8 {
                    let val = i64::from_le_bytes(data[..8].try_into().unwrap());
                    (Value::Int(val), 8)
                } else {
                    (Value::Null, 0)
                }
            }
            super::constants::MYSQL_DOUBLE => {
                if data.len() >= 8 {
                    let val = f64::from_le_bytes(data[..8].try_into().unwrap());
                    (Value::Float(val), 8)
                } else {
                    (Value::Null, 0)
                }
            }
            _ => {
                let (s, n) = read_len_enc_string_from(data);
                (Value::Text(s), n)
            }
        };
        params.push(value);
    }
    params
}

pub fn read_len_enc_string_from(data: &[u8]) -> (String, usize) {
    if data.is_empty() {
        return (String::new(), 0);
    }

    match data[0] {
        super::constants::LEN_ENC_2BYTE => {
            if data.len() >= 3 {
                let len = u16::from_le_bytes([data[1], data[2]]) as usize;
                let s = String::from_utf8_lossy(&data[3..3 + min(len, data.len() - 3)]).to_string();
                (s, 3 + len)
            } else {
                (String::new(), data.len())
            }
        }
        super::constants::LEN_ENC_3BYTE => {
            if data.len() >= 4 {
                let len = u32::from_le_bytes([data[1], data[2], data[3], 0]) as usize;
                let s = String::from_utf8_lossy(&data[4..min(4 + len, data.len())]).to_string();
                (s, 4 + len)
            } else {
                (String::new(), data.len())
            }
        }
        super::constants::LEN_ENC_8BYTE => (String::new(), data.len()),
        0 => (String::new(), 1),
        c if c < 252 => {
            let len = c as usize;
            let s = String::from_utf8_lossy(&data[1..min(1 + len, data.len())]).to_string();
            (s, 1 + len)
        }
        _ => (String::new(), data.len()),
    }
}

fn min(a: usize, b: usize) -> usize {
    if a < b { a } else { b }
}
