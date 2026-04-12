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
    pkt.push(0xFF);
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
        "root".to_string()
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
            0x01 => break, // COM_QUIT
            0x03 => {
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
            0x02 => {
                // COM_INIT_DB
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
            0x04 => {
                // COM_FIELD_LIST - List columns of a table
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
            0x0A => {
                // COM_STATISTICS
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
            0x0E => {
                // COM_PING
                send_ok(&mut socket, seq + 1).await?;
            }
            0x16 => {
                // COM_STMT_PREPARE
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
                payload.push(0x00); // OK header
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
            0x17 => {
                // COM_STMT_EXECUTE
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
            0x19 => {
                // COM_STMT_CLOSE
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
                send_error(&mut socket, seq + 1, 1047, "08S01", "Unknown command").await?;
            }
        }
    }

    Ok(())
}

async fn send_handshake(socket: &mut TcpStream) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(10); // Protocol version
    payload.extend_from_slice(b"ThySqueal-0.8.0\0");
    payload.extend_from_slice(&[0u8; 4]); // Connection ID (dummy)
    payload.extend_from_slice(b"authplug\0"); // Auth plugin data part 1
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0xF7FF)?; // Capability flags (lower)
    payload.push(33); // Character set (utf8_general_ci)
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0x0002)?; // Status flags
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0x8000)?; // Capability flags (upper)
    payload.push(0); // Auth plugin data length
    payload.extend_from_slice(&[0u8; 10]); // Reserved
    payload.extend_from_slice(b"authplug\0"); // Auth plugin data part 2
    payload.extend_from_slice(b"mysql_native_password\0");

    send_packet(socket, 0, &payload).await
}

async fn send_ok(socket: &mut TcpStream, seq: u8) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(0x00); // OK header
    payload.push(0); // Affected rows
    payload.push(0); // Last insert ID
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0x0002)?; // Status flags
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0x0000)?; // Warnings

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
    payload.push(0xFF); // Error header
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, code)?;
    payload.push(b'#'); // SQL State marker
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
        payload.push(0x0C); // Length of fixed-length fields
        WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 33)?; // Character set
        WriteBytesExt::write_u32::<LittleEndian>(&mut payload, 255)?; // Column length
        payload.push(0xFD); // Type (VAR_STRING)
        WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?; // Flags
        payload.push(0x00); // Decimals
        WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?; // Filter

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
                Value::Null => payload.push(0xFB),
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
    payload.push(0xFE); // EOF header
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?; // Warnings
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0x0002)?; // Status flags

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
    payload.push(0x00); // Decimals
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?; // Filter

    send_packet(socket, 0, &payload).await
}

async fn parse_result_columns(query: &str) -> Result<Vec<ColumnMeta>> {
    let query_lower = query.to_uppercase();
    if query_lower.starts_with("SELECT") || query_lower.starts_with("WITH") {
        Ok(vec![ColumnMeta {
            name: "".to_string(),
            type_code: 0xFD,
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
                type_code: 0xFD, // VARCHAR - would need proper type inference
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
        Value::Null => 0x06,        // NULL_TYPE
        Value::Int(_) => 0x03,      // INT
        Value::Float(_) => 0x04,    // FLOAT
        Value::Text(_) => 0xFD,     // VARCHAR
        Value::Bool(_) => 0x01,     // TINYINT
        Value::DateTime(_) => 0x0C, // DATETIME
        Value::Json(_) => 0xFE,     // BLOB (treat JSON as blob)
    }
}

impl Value {
    #[allow(dead_code)]
    pub fn to_mysql_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => vec![0xFB],
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

        // Check for NULL (0xFB in MySQL binary protocol)
        if data[offset] == 0xFB {
            values.push(Value::Null);
            offset += 1;
            continue;
        }

        // Extract value based on type
        let value = match param.type_code {
            0x01 => {
                // TINYINT
                if offset < data.len() {
                    let v = data[offset] as i64;
                    offset += 1;
                    Value::Int(v)
                } else {
                    Value::Null
                }
            }
            0x02 => {
                // SMALLINT
                if offset + 1 < data.len() {
                    let v = i16::from_le_bytes([data[offset], data[offset + 1]]) as i64;
                    offset += 2;
                    Value::Int(v)
                } else {
                    Value::Null
                }
            }
            0x03 => {
                // INT
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
            0x08 => {
                // BIGINT
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
            0xFD | 0xFE => {
                // VARCHAR/VARSTRING - read length-encoded string
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
        0xFC => {
            if data.len() >= 3 {
                u16::from_le_bytes([data[1], data[2]]) as usize
            } else {
                return (String::new(), data.len());
            }
        }
        0xFD => {
            if data.len() >= 4 {
                u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize
            } else {
                return (String::new(), data.len());
            }
        }
        0xFE => {
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
        0xFC => 3,
        0xFD => 5,
        0xFE => 9,
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
