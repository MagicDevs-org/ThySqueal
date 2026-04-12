use super::packet::*;
use crate::engines::mysql::error::SqlError;
use crate::squeal::exec::{Executor, QueryResult, Session};
use crate::storage::Value;
use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::info;

const PROTO_VERSION: u8 = 10;
const DEFAULT_USERNAME: &str = "root";
const AUTH_PLUGIN_NAME: &str = "mysql_native_password";
const AUTH_PLUGIN_DATA_PART1: &str = "authplug";
const AUTH_PLUGIN_DATA_PART2: &str = "authplug";
const SERVER_VERSION: &str = "ThySqueal-0.8.0";

const CHAR_SET_UTF8: u16 = 33;
const CHAR_SET_CODE: u8 = 33;
const STATUS_FLAGS: u16 = 0x0002;

const CAPABILITY_LOWER: u16 = 0xF7FF;
const CAPABILITY_UPPER: u16 = 0x8000;

const MYSQL_NULL_TYPE: u8 = 0x06;
const MYSQL_TINYINT: u8 = 0x01;
const MYSQL_SMALLINT: u8 = 0x02;
const MYSQL_INT: u8 = 0x03;
const MYSQL_FLOAT: u8 = 0x04;
const MYSQL_BIGINT: u8 = 0x08;
const MYSQL_DATETIME: u8 = 0x0C;
const MYSQL_VAR_STRING: u8 = 0xFD;
const MYSQL_BLOB: u8 = 0xFE;

const MYSQL_NULL_IN_BIND: u8 = 0xFB;
const MYSQL_OK_HEADER: u8 = 0x00;
const MYSQL_ERROR_HEADER: u8 = 0xFF;
const MYSQL_EOF_HEADER: u8 = 0xFE;

const LEN_ENC_2BYTE: u8 = 0xFC;
const LEN_ENC_3BYTE: u8 = 0xFD;
const LEN_ENC_8BYTE: u8 = 0xFE;

const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_FIELD_LIST: u8 = 0x04;
const COM_STATISTICS: u8 = 0x0A;
const COM_PING: u8 = 0x0E;
const COM_STMT_PREPARE: u8 = 0x16;
const COM_STMT_EXECUTE: u8 = 0x17;
const COM_STMT_CLOSE: u8 = 0x19;

const ERR_CODE_UNKNOWN_CMD: u16 = 1047;
const ERR_SQL_STATE: &str = "08S01";

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

async fn send_sql_error(socket: &mut TcpStream, seq: u8, err: &SqlError) -> Result<()> {
    let payload = err.to_string();
    let code = err.mysql_errno();
    let state = err.mysql_sqlstate();
    let mut pkt = Vec::new();
    pkt.push(MYSQL_ERROR_HEADER);
    WriteBytesExt::write_u16::<LittleEndian>(&mut pkt, code)?;
    pkt.push(b'#');
    pkt.extend_from_slice(state.as_bytes());
    pkt.extend_from_slice(payload.as_bytes());
    send_packet(socket, seq, &pkt).await
}

pub async fn handle_connection(mut socket: TcpStream, executor: Arc<Executor>) -> Result<()> {
    // 1. Send Initial Handshake Packet
    send_handshake(&mut socket).await?;

    // 2. Receive Handshake Response
    let (_seq, payload) = read_packet(&mut socket).await?;
    // Handshake response contains username at offset 32 (after capability flags, charset, reserved)
    // It's a null-terminated string.
    let username = if payload.len() > 32 {
        let user_bytes: Vec<u8> = payload[32..]
            .iter()
            .take_while(|&&b| b != 0)
            .cloned()
            .collect();
        String::from_utf8_lossy(&user_bytes).to_string()
    } else {
        DEFAULT_USERNAME.to_string()
    };

    // For now, we accept any credentials (no auth)
    send_ok(&mut socket, 0).await?;

    // Statement cache (in-memory for now)
    let mut stmt_cache: HashMap<u64, PreparedStatement> = HashMap::new();
    let mut next_stmt_id: u64 = 1;

    // 3. Command Loop
    loop {
        let (seq, payload) = match read_packet(&mut socket).await {
            Ok(p) => p,
            Err(_) => break, // Connection closed
        };

        if payload.is_empty() {
            break;
        }

        let command = payload[0];
        let data = &payload[1..];

        match command {
            COM_QUIT => break,
            COM_QUERY => {
                // COM_QUERY
                let query = match std::str::from_utf8(data) {
                    Ok(q) => q,
                    Err(_) => {
                        let err = SqlError::Parse("Invalid UTF-8 query".to_string());
                        send_sql_error(&mut socket, seq + 1, &err).await?;
                        continue;
                    }
                };
                let session = Session::new(Some(username.clone()), None);
                match executor.execute(query, vec![], session).await {
                    Ok(result) => {
                        if result.rows.is_empty() {
                            send_ok(&mut socket, seq + 1).await?;
                        } else {
                            send_result_set(&mut socket, seq + 1, result).await?;
                        }
                    }
                    Err(e) => send_sql_error(&mut socket, seq + 1, &e.into()).await?,
                }
            }
            COM_INIT_DB => {
                let db_name = match std::str::from_utf8(data) {
                    Ok(s) => s.trim_end_matches('\0').to_string(),
                    Err(_) => {
                        let err = SqlError::Runtime("Invalid database name".to_string());
                        send_sql_error(&mut socket, seq + 1, &err).await?;
                        continue;
                    }
                };
                let session = Session::new(Some(username.clone()), Some(db_name));
                match executor.execute("SELECT 1", vec![], session).await {
                    Ok(_) => send_ok(&mut socket, seq + 1).await?,
                    Err(e) => send_sql_error(&mut socket, seq + 1, &e.into()).await?,
                }
            }
            COM_FIELD_LIST => {
                let payload_str = match std::str::from_utf8(data) {
                    Ok(s) => s.trim_end_matches('\0').to_string(),
                    Err(_) => {
                        let err = SqlError::Runtime("Invalid table name".to_string());
                        send_sql_error(&mut socket, seq + 1, &err).await?;
                        continue;
                    }
                };
                let parts: Vec<&str> = payload_str.split('\0').collect();
                let table_name = parts.first().unwrap_or(&"");

                let query = format!(
                    "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, EXTRA \
                     FROM information_schema.COLUMNS WHERE TABLE_NAME = '{}' ORDER BY ORDINAL_POSITION",
                    table_name
                );
                let session = Session::new(Some(username.clone()), None);
                match executor.execute(&query, vec![], session).await {
                    Ok(result) => send_result_set(&mut socket, seq + 1, result).await?,
                    Err(e) => send_sql_error(&mut socket, seq + 1, &e.into()).await?,
                }
            }
            COM_STATISTICS => {
                let session = Session::new(Some(username.clone()), None);
                let stats = match executor
                    .execute(
                        "SELECT 'Uptime' as Variable_name, 3600 as Value \
                     UNION ALL SELECT 'Threads_connected', 1 \
                     UNION ALL SELECT 'Questions', 100",
                        vec![],
                        session,
                    )
                    .await
                {
                    Ok(result) => result
                        .rows
                        .into_iter()
                        .map(|row| {
                            row.into_iter()
                                .map(|v| v.to_string())
                                .collect::<Vec<_>>()
                                .join("\t")
                        })
                        .collect::<Vec<_>>()
                        .join("\n"),
                    Err(_) => String::new(),
                };
                let payload = stats.as_bytes().to_vec();
                send_packet(&mut socket, seq + 1, &payload).await?;
            }
            COM_PING => {
                send_ok(&mut socket, seq + 1).await?;
            }
            COM_STMT_PREPARE => {
                let query = match std::str::from_utf8(data) {
                    Ok(q) => q.trim_end_matches('\0'),
                    Err(_) => {
                        send_error(&mut socket, seq + 1, 1105, "HY000", "Invalid UTF-8 query")
                            .await?;
                        continue;
                    }
                };

                let stmt_id = next_stmt_id;
                next_stmt_id += 1;

                let columns = parse_result_columns(query).await.unwrap_or_default();
                let params = parse_parameter_columns(query);

                stmt_cache.insert(
                    stmt_id,
                    PreparedStatement {
                        id: stmt_id,
                        query: query.to_string(),
                        columns: columns.clone(),
                        params: params.clone(),
                    },
                );

                let mut payload = Vec::new();
                payload.push(MYSQL_OK_HEADER);
                write_len_enc_int(&mut payload, stmt_id); // Statement ID
                write_len_enc_int(&mut payload, columns.len() as u64); // Column count
                write_len_enc_int(&mut payload, params.len() as u64); // Param count
                payload.push(0); // Reserved
                send_packet(&mut socket, seq + 1, &payload).await?;

                // Send param definitions
                for param in &params {
                    send_column_definition(&mut socket, param).await?;
                }
                send_eof(&mut socket, seq + 2).await?;

                // Send column definitions
                for col in &columns {
                    send_column_definition(&mut socket, col).await?;
                }
                send_eof(&mut socket, seq + 3).await?;
            }
            COM_STMT_EXECUTE => {
                // First byte after command is statement ID
                let stmt_id = if data.len() >= 8 {
                    u64::from_le_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])
                } else {
                    send_error(&mut socket, seq + 1, 1105, "HY000", "Invalid statement ID").await?;
                    continue;
                };

                let stmt = match stmt_cache.get(&stmt_id) {
                    Some(s) => s,
                    None => {
                        send_error(&mut socket, seq + 1, 1105, "HY000", "Unknown statement")
                            .await?;
                        continue;
                    }
                };

                // TODO: properly handle null_bitmap for null values in COM_STMT_EXECUTE
                let bound_data_start = 8 + stmt.params.len().div_ceil(8);
                let params = if data.len() > bound_data_start {
                    extract_bound_params(&data[bound_data_start..], &stmt.params).await
                } else {
                    vec![]
                };

                let session = Session::new(Some(username.clone()), None);
                match executor.execute(&stmt.query, params, session).await {
                    Ok(result) => {
                        if result.rows.is_empty() {
                            send_ok(&mut socket, seq + 1).await?;
                        } else {
                            send_result_set(&mut socket, seq + 1, result).await?;
                        }
                    }
                    Err(e) => send_sql_error(&mut socket, seq + 1, &e.into()).await?,
                }
            }
            COM_STMT_CLOSE => {
                let stmt_id = if data.len() >= 8 {
                    u64::from_le_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])
                } else {
                    0
                };
                stmt_cache.remove(&stmt_id);
                send_ok(&mut socket, seq + 1).await?;
            }
            _ => {
                info!("Unsupported MySQL command: 0x{:02X}", command);
                send_error(
                    &mut socket,
                    seq + 1,
                    ERR_CODE_UNKNOWN_CMD,
                    ERR_SQL_STATE,
                    "Unknown command",
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn send_handshake(socket: &mut TcpStream) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(PROTO_VERSION);
    payload.extend_from_slice(SERVER_VERSION.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&[0u8; 4]);
    payload.extend_from_slice(AUTH_PLUGIN_DATA_PART1.as_bytes());
    payload.push(0);
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, CAPABILITY_LOWER)?;
    payload.push(CHAR_SET_CODE);
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, STATUS_FLAGS)?;
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, CAPABILITY_UPPER)?;
    payload.push(0);
    payload.extend_from_slice(&[0u8; 10]);
    payload.extend_from_slice(AUTH_PLUGIN_DATA_PART2.as_bytes());
    payload.push(0);
    payload.extend_from_slice(AUTH_PLUGIN_NAME.as_bytes());
    payload.push(0);

    send_packet(socket, 0, &payload).await
}

async fn send_ok(socket: &mut TcpStream, seq: u8) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(MYSQL_OK_HEADER);
    payload.push(0);
    payload.push(0);
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, STATUS_FLAGS)?;
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?;

    send_packet(socket, seq, &payload).await
}

async fn send_error(
    socket: &mut TcpStream,
    seq: u8,
    code: u16,
    state: &str,
    msg: &str,
) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(MYSQL_ERROR_HEADER);
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, code)?;
    payload.push(b'#');
    payload.extend_from_slice(state.as_bytes());
    payload.extend_from_slice(msg.as_bytes());

    send_packet(socket, seq, &payload).await
}

async fn send_result_set(socket: &mut TcpStream, mut seq: u8, result: QueryResult) -> Result<()> {
    // 1. Column Count
    let mut payload = Vec::new();
    write_len_enc_int(&mut payload, result.columns.len() as u64);
    send_packet(socket, seq, &payload).await?;
    seq += 1;

    // 2. Column Definitions
    for col_name in &result.columns {
        let mut payload = Vec::new();
        write_len_enc_str(&mut payload, "def"); // Catalog
        write_len_enc_str(&mut payload, ""); // Schema
        write_len_enc_str(&mut payload, ""); // Table
        write_len_enc_str(&mut payload, ""); // Org Table
        write_len_enc_str(&mut payload, col_name); // Name
        write_len_enc_str(&mut payload, col_name); // Org Name
        payload.push(MYSQL_DATETIME);
        WriteBytesExt::write_u16::<LittleEndian>(&mut payload, CHAR_SET_UTF8)?;
        WriteBytesExt::write_u32::<LittleEndian>(&mut payload, 255)?;
        payload.push(MYSQL_VAR_STRING);
        WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?;
        payload.push(0);
        WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?;

        send_packet(socket, seq, &payload).await?;
        seq += 1;
    }

    // 3. EOF Packet
    send_eof(socket, seq).await?;
    seq += 1;

    // 4. Row Data
    for row in result.rows {
        let mut payload = Vec::new();
        for val in row {
            match val {
                Value::Null => payload.push(MYSQL_NULL_IN_BIND),
                _ => write_len_enc_str(&mut payload, &val.to_string()),
            }
        }
        send_packet(socket, seq, &payload).await?;
        seq += 1;
    }

    // 5. Final EOF Packet
    send_eof(socket, seq).await?;

    Ok(())
}

async fn send_eof(socket: &mut TcpStream, seq: u8) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(MYSQL_EOF_HEADER);
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?;
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, STATUS_FLAGS)?;

    send_packet(socket, seq, &payload).await
}

async fn send_column_definition(socket: &mut TcpStream, col: &ColumnMeta) -> Result<()> {
    let mut payload = Vec::new();
    write_len_enc_str(&mut payload, "def"); // Catalog
    write_len_enc_str(&mut payload, ""); // Schema
    write_len_enc_str(&mut payload, ""); // Table
    write_len_enc_str(&mut payload, ""); // Org Table
    write_len_enc_str(&mut payload, &col.name); // Name
    write_len_enc_str(&mut payload, &col.name); // Org Name
    payload.push(0x0C); // Length of fixed-length fields
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 33)?; // Character set
    WriteBytesExt::write_u32::<LittleEndian>(&mut payload, 255)?; // Column length
    payload.push(col.type_code); // Type
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, col.flags)?; // Flags
    payload.push(0); // Decimals
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?; // Filter

    send_packet(socket, 0, &payload).await
}

async fn parse_result_columns(query: &str) -> Result<Vec<ColumnMeta>> {
    let query_lower = query.to_uppercase();
    if query_lower.starts_with("SELECT") || query_lower.starts_with("WITH") {
        Ok(vec![ColumnMeta {
            name: "".to_string(),
            type_code: MYSQL_VAR_STRING,
            flags: 0,
        }])
    } else {
        Ok(vec![])
    }
}

fn parse_parameter_columns(query: &str) -> Vec<ColumnMeta> {
    let mut params = Vec::new();
    let mut param_idx = 1;
    let query_bytes = query.as_bytes();

    let mut i = 0;
    while i < query_bytes.len() {
        let b = query_bytes[i];
        if b == b'?' {
            params.push(ColumnMeta {
                name: format!("param_{}", param_idx),
                type_code: MYSQL_VAR_STRING,
                flags: 0,
            });
            param_idx += 1;
        } else if b == b'\'' {
            i += 1;
            while i < query_bytes.len() && query_bytes[i] != b'\'' {
                if query_bytes[i] == b'\\' && i + 1 < query_bytes.len() {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            i += 1;
        } else {
            i += 1;
        }
    }

    params
}

#[allow(dead_code)]
pub fn value_to_mysql_type(value: &Value) -> u8 {
    match value {
        Value::Null => MYSQL_NULL_TYPE,
        Value::Int(_) => MYSQL_INT,
        Value::Float(_) => MYSQL_FLOAT,
        Value::Text(_) => MYSQL_VAR_STRING,
        Value::Bool(_) => MYSQL_TINYINT,
        Value::DateTime(_) => MYSQL_DATETIME,
        Value::Json(_) => MYSQL_BLOB,
    }
}

impl Value {
    #[allow(dead_code)]
    pub fn to_mysql_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => vec![MYSQL_NULL_IN_BIND],
            Value::Int(i) => {
                let mut bytes = vec![];
                WriteBytesExt::write_i64::<LittleEndian>(&mut bytes, *i).unwrap();
                bytes
            }
            Value::Float(f) => {
                let mut bytes = vec![];
                WriteBytesExt::write_f32::<LittleEndian>(&mut bytes, *f as f32).unwrap();
                bytes
            }
            Value::Text(s) => {
                let mut bytes = vec![];
                write_len_enc_str(&mut bytes, s);
                bytes
            }
            Value::Bool(b) => vec![if *b { 1 } else { 0 }],
            Value::DateTime(dt) => {
                let mut bytes = vec![];
                write_len_enc_str(&mut bytes, &dt.to_rfc3339());
                bytes
            }
            Value::Json(j) => {
                let mut bytes = vec![];
                write_len_enc_str(&mut bytes, &j.to_string());
                bytes
            }
        }
    }
}

async fn extract_bound_params(data: &[u8], param_types: &[ColumnMeta]) -> Vec<Value> {
    let mut values = Vec::new();
    let mut offset = 0;

    for param in param_types {
        if offset >= data.len() {
            break;
        }

        if data[offset] == MYSQL_NULL_IN_BIND {
            values.push(Value::Null);
            offset += 1;
            continue;
        }

        // Extract value based on type
        let value = match param.type_code {
            MYSQL_TINYINT => {
                if offset < data.len() {
                    let v = data[offset] as i64;
                    offset += 1;
                    Value::Int(v)
                } else {
                    Value::Null
                }
            }
            MYSQL_SMALLINT => {
                if offset + 1 < data.len() {
                    let v = i16::from_le_bytes([data[offset], data[offset + 1]]) as i64;
                    offset += 2;
                    Value::Int(v)
                } else {
                    Value::Null
                }
            }
            MYSQL_INT => {
                if offset + 3 < data.len() {
                    let v = i32::from_le_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]) as i64;
                    offset += 4;
                    Value::Int(v)
                } else {
                    Value::Null
                }
            }
            MYSQL_BIGINT => {
                if offset + 7 < data.len() {
                    let v = i64::from_le_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                        data[offset + 4],
                        data[offset + 5],
                        data[offset + 6],
                        data[offset + 7],
                    ]);
                    offset += 8;
                    Value::Int(v)
                } else {
                    Value::Null
                }
            }
            MYSQL_VAR_STRING | MYSQL_BLOB => {
                let (val, new_offset) = read_len_enc_string_from(&data[offset..]);
                offset += new_offset;
                Value::Text(val)
            }
            _ => {
                // Default to string
                let (val, new_offset) = read_len_enc_string_from(&data[offset..]);
                offset += new_offset;
                Value::Text(val)
            }
        };
        values.push(value);
    }

    values
}

fn read_len_enc_string_from(data: &[u8]) -> (String, usize) {
    if data.is_empty() {
        return (String::new(), 0);
    }

    let len = match data[0] {
        LEN_ENC_2BYTE => {
            if data.len() >= 3 {
                u16::from_le_bytes([data[1], data[2]]) as usize
            } else {
                return (String::new(), data.len());
            }
        }
        LEN_ENC_3BYTE => {
            if data.len() >= 4 {
                u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize
            } else {
                return (String::new(), data.len());
            }
        }
        LEN_ENC_8BYTE => {
            if data.len() >= 9 {
                u64::from_le_bytes([
                    data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
                ]) as usize
            } else {
                return (String::new(), data.len());
            }
        }
        b if b < 251 => data[0] as usize,
        _ => return (String::new(), data.len()),
    };

    let start = match data[0] {
        LEN_ENC_2BYTE => 3,
        LEN_ENC_3BYTE => 5,
        LEN_ENC_8BYTE => 9,
        _ => 1,
    };

    if start + len <= data.len() {
        (
            String::from_utf8_lossy(&data[start..start + len]).to_string(),
            start + len,
        )
    } else {
        (String::new(), data.len())
    }
}
