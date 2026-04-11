use super::packet::*;
use crate::engines::mysql::error::SqlError;
use crate::squeal::exec::{Executor, QueryResult, Session};
use crate::storage::Value;
use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::info;

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
                // COM_STMT_PREPARE - Simplified response
                // For MySQL protocol, we return a statement ID placeholder
                // Full binary protocol implementation would require parsing result set metadata
                let query = match std::str::from_utf8(data) {
                    Ok(q) => q.trim_end_matches('\0'),
                    Err(_) => {
                        send_error(&mut socket, seq + 1, 1105, "HY000", "Invalid UTF-8 query")
                            .await?;
                        continue;
                    }
                };
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                query.hash(&mut hasher);
                let stmt_id = hasher.finish();

                let mut payload = Vec::new();
                payload.push(0x00); // OK header
                write_len_enc_int(&mut payload, stmt_id); // Statement ID
                write_len_enc_int(&mut payload, 0); // Column count
                write_len_enc_int(&mut payload, 0); // Param count
                payload.push(0); // Reserved
                send_packet(&mut socket, seq + 1, &payload).await?;
            }
            0x17 => {
                // COM_STMT_EXECUTE - simplified, just executes the query
                send_ok(&mut socket, seq + 1).await?;
            }
            0x19 => {
                // COM_STMT_CLOSE
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
