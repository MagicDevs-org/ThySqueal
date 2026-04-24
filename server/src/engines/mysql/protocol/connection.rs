use std::collections::HashMap;
use tokio::net::TcpStream;

use super::constants::*;
use super::packet::*;
use super::types::*;
use crate::engines::mysql::error::SqlError;
use crate::squeal::exec::{Executor, Session};
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

pub async fn handle_connection(mut socket: TcpStream, executor: Arc<Executor>) -> Result<()> {
    let challenge = send_handshake(&mut socket).await?;

    let (seq, payload) = read_packet(&mut socket).await?;

    let (username, auth_response) = parse_handshake_response(&payload);

    let db = executor.db.read().await;
    let auth_ok = if let Some(user) = db.state().users.get(&username) {
        if user.password_hash.is_empty() && user.auth_string.is_none() {
            true
        } else if let Some(ref auth_string) = user.auth_string {
            auth_response.as_ref().map_or(false, |r| {
                verify_mysql_native_password(&challenge, auth_string, r)
            })
        } else {
            false
        }
    } else {
        username.is_empty() || username == DEFAULT_USERNAME
    };
    drop(db);

    if auth_ok {
        executor.metrics.inc_connection();
        send_ok_packet(&mut socket, seq + 1, "Welcome to ThySqueal").await?;
    } else {
        send_sql_error(
            &mut socket,
            seq + 1,
            &SqlError::Runtime("Authentication failed".to_string()),
        )
        .await?;
        return Ok(());
    }

    process_commands(&mut socket, &executor).await
}

fn verify_mysql_native_password(challenge: &str, stored_hash: &str, response: &[u8]) -> bool {
    use sha1::{Digest, Sha1};

    if response.len() != 20 {
        return false;
    }

    let hash_bytes = match hex::decode(stored_hash) {
        Ok(h) if h.len() == 20 => h,
        _ => return false,
    };

    let mut sha1 = Sha1::new();
    sha1.update(response);
    sha1.update(challenge.as_bytes());
    let step1 = sha1.finalize();

    let mut sha1 = Sha1::new();
    sha1.update(step1);
    let step2 = sha1.finalize();

    for i in 0..20 {
        if step2[i] != hash_bytes[i] {
            return false;
        }
    }

    true
}

async fn process_commands(socket: &mut TcpStream, executor: &Arc<Executor>) -> Result<()> {
    let mut prepared_statements: HashMap<u64, PreparedStatement> = HashMap::new();
    let mut stmt_id_counter: u64 = 0;

    loop {
        match read_packet(socket).await {
            Ok((seq, payload)) => {
                if payload.is_empty() {
                    continue;
                }

                let cmd = payload[0];

                match cmd {
                    COM_QUIT => {
                        info!("MySQL client disconnected");
                        return Ok(());
                    }
                    COM_STMT_CLOSE => {
                        handle_stmt_close(&payload, &mut prepared_statements);
                        continue;
                    }
                    _ => {
                        let seq_num = seq + 1;
                        match cmd {
                            COM_PING => {
                                send_ok_packet(socket, seq_num, "PONG").await?;
                            }
                            COM_INIT_DB => {
                                handle_init_db(socket, executor, seq_num, &payload).await?;
                            }
                            COM_QUERY => {
                                handle_query(socket, executor, seq_num, &payload).await?;
                            }
                            COM_STMT_PREPARE => {
                                handle_stmt_prepare(
                                    socket,
                                    seq_num,
                                    &payload,
                                    &mut prepared_statements,
                                    &mut stmt_id_counter,
                                )
                                .await?;
                            }
                            COM_STMT_EXECUTE => {
                                handle_stmt_execute(
                                    socket,
                                    executor,
                                    seq_num,
                                    &payload,
                                    &prepared_statements,
                                )
                                .await?;
                            }
                            COM_CREATE_DB => {
                                handle_create_db(socket, executor, seq_num, &payload).await?;
                            }
                            COM_DROP_DB => {
                                handle_drop_db(socket, executor, seq_num, &payload).await?;
                            }
                            COM_FIELD_LIST => {
                                handle_field_list(socket, executor, seq_num, &payload).await?;
                            }
                            COM_KILL => {
                                handle_kill(socket, executor, seq_num, &payload).await?;
                            }
                            COM_STATISTICS => {
                                send_ok_packet(
                                    socket,
                                    seq_num,
                                    "Threads: 1  Questions: 0  Slow: 0",
                                )
                                .await?;
                            }
                            COM_DEBUG | COM_TIME => {
                                send_ok_packet(socket, seq_num, "").await?;
                            }
                            _ => {
                                send_sql_error(
                                    socket,
                                    seq_num,
                                    &SqlError::Parse(format!("Unknown command: {}", cmd)),
                                )
                                .await?;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                info!("MySQL connection error: {}", e);
                return Ok(());
            }
        }
    }
}

pub fn parse_handshake_response(payload: &[u8]) -> (String, Option<Vec<u8>>) {
    let _capability: u32 = if payload.len() >= 4 {
        u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]])
    } else {
        0
    };

    let _charset = if payload.len() > 4 {
        payload[4]
    } else {
        CHAR_SET_CODE
    };

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

    let password_start = 32 + username.len() + 1;
    let password_hash: Option<Vec<u8>> =
        if payload.len() > password_start && payload[password_start] != 0 {
            let pass_bytes: Vec<u8> = payload[password_start..]
                .iter()
                .take_while(|&&b| b != 0)
                .cloned()
                .collect();
            if pass_bytes.len() == 20 {
                Some(pass_bytes)
            } else {
                None
            }
        } else {
            None
        };

    (username, password_hash)
}

async fn handle_init_db(
    socket: &mut TcpStream,
    executor: &Arc<Executor>,
    seq_num: u8,
    payload: &[u8],
) -> Result<()> {
    let db_name = String::from_utf8_lossy(&payload[1..]).to_string();
    let result = executor
        .execute(
            &format!("USE {}", db_name),
            vec![],
            Session::new(None, None),
        )
        .await;
    match result {
        Ok(_) => send_ok_packet(socket, seq_num, "").await?,
        Err(e) => {
            send_sql_error(socket, seq_num, &SqlError::from(e)).await?;
        }
    }
    Ok(())
}

async fn handle_query(
    socket: &mut TcpStream,
    executor: &Arc<Executor>,
    seq_num: u8,
    payload: &[u8],
) -> Result<()> {
    let query = String::from_utf8_lossy(&payload[1..]).to_string();
    executor.metrics.inc_question();
    executor.metrics.inc_query();

    let query_upper = query.to_uppercase();
    if query_upper.trim_start().starts_with("SELECT") {
        executor.metrics.inc_select();
    } else if query_upper.trim_start().starts_with("INSERT") {
        executor.metrics.inc_insert();
    } else if query_upper.trim_start().starts_with("UPDATE") {
        executor.metrics.inc_update();
    } else if query_upper.trim_start().starts_with("DELETE") {
        executor.metrics.inc_delete();
    }

    let result = executor
        .execute(&query, vec![], Session::new(None, None))
        .await;
    match result {
        Ok(res) => {
            send_resultset(socket, seq_num, res).await?;
        }
        Err(e) => {
            send_sql_error(socket, seq_num, &SqlError::from(e)).await?;
        }
    }
    Ok(())
}

async fn handle_stmt_prepare(
    socket: &mut TcpStream,
    seq_num: u8,
    payload: &[u8],
    prepared_statements: &mut HashMap<u64, PreparedStatement>,
    stmt_id_counter: &mut u64,
) -> Result<()> {
    let query = String::from_utf8_lossy(&payload[1..]).to_string();
    *stmt_id_counter += 1;
    let stmt_id = *stmt_id_counter;
    let columns = parse_parameter_columns(&query);
    let params: Vec<ColumnMeta> = columns
        .iter()
        .filter(|c| c.name.starts_with("@"))
        .cloned()
        .collect();
    let columns: Vec<ColumnMeta> = columns
        .into_iter()
        .filter(|c| !c.name.starts_with("@"))
        .collect();

    let num_columns = columns.len() as u16;
    let num_params = params.len() as u16;

    let stmt = PreparedStatement {
        id: stmt_id,
        query: query.clone(),
        columns,
        params,
    };
    prepared_statements.insert(stmt_id, stmt);

    send_stmt_prepare_ok(socket, seq_num, stmt_id, num_columns, num_params).await?;
    Ok(())
}

async fn handle_stmt_execute(
    socket: &mut TcpStream,
    executor: &Arc<Executor>,
    seq_num: u8,
    payload: &[u8],
    prepared_statements: &HashMap<u64, PreparedStatement>,
) -> Result<()> {
    if payload.len() < 5 {
        send_sql_error(
            socket,
            seq_num,
            &SqlError::Parse("Invalid statement execute".to_string()),
        )
        .await?;
        return Ok(());
    }

    let stmt_id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
    let stmt = match prepared_statements.get(&(stmt_id as u64)) {
        Some(s) => s.clone(),
        None => {
            send_sql_error(
                socket,
                seq_num,
                &SqlError::Parse(format!("Unknown statement id: {}", stmt_id)),
            )
            .await?;
            return Ok(());
        }
    };

    let null_bitmap_offset = 9;
    let num_params = stmt.params.len();
    let bound_params = if num_params > 0 {
        let null_bitmap_len = num_params.div_ceil(8);
        extract_bound_params(
            &payload[null_bitmap_offset + null_bitmap_len..],
            &stmt.params,
            &payload[null_bitmap_offset..null_bitmap_offset + null_bitmap_len],
        )
    } else {
        vec![]
    };

    let result = executor
        .execute(&stmt.query, bound_params, Session::new(None, None))
        .await;
    match result {
        Ok(res) => send_resultset(socket, seq_num, res).await?,
        Err(e) => {
            send_sql_error(socket, seq_num, &SqlError::from(e)).await?;
        }
    }
    Ok(())
}

fn handle_stmt_close(payload: &[u8], prepared_statements: &mut HashMap<u64, PreparedStatement>) {
    if payload.len() >= 5 {
        let stmt_id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
        prepared_statements.remove(&(stmt_id as u64));
    }
}

async fn handle_create_db(
    socket: &mut TcpStream,
    executor: &Arc<Executor>,
    seq_num: u8,
    payload: &[u8],
) -> Result<()> {
    let db_name = String::from_utf8_lossy(&payload[1..]).to_string();
    let result = executor
        .execute(
            &format!("CREATE DATABASE {}", db_name),
            vec![],
            Session::new(None, None),
        )
        .await;
    match result {
        Ok(_) => send_ok_packet(socket, seq_num, "").await?,
        Err(e) => {
            send_sql_error(socket, seq_num, &SqlError::from(e)).await?;
        }
    }
    Ok(())
}

async fn handle_drop_db(
    socket: &mut TcpStream,
    executor: &Arc<Executor>,
    seq_num: u8,
    payload: &[u8],
) -> Result<()> {
    let db_name = String::from_utf8_lossy(&payload[1..]).to_string();
    let result = executor
        .execute(
            &format!("DROP DATABASE {}", db_name),
            vec![],
            Session::new(None, None),
        )
        .await;
    match result {
        Ok(_) => send_ok_packet(socket, seq_num, "").await?,
        Err(e) => {
            send_sql_error(socket, seq_num, &SqlError::from(e)).await?;
        }
    }
    Ok(())
}

async fn handle_field_list(
    socket: &mut TcpStream,
    executor: &Arc<Executor>,
    seq_num: u8,
    payload: &[u8],
) -> Result<()> {
    let table_name = String::from_utf8_lossy(&payload[1..]).to_string();
    let result = executor
        .execute(
            &format!("SHOW COLUMNS FROM {}", table_name),
            vec![],
            Session::new(None, None),
        )
        .await;
    match result {
        Ok(res) => send_resultset(socket, seq_num, res).await?,
        Err(e) => {
            send_sql_error(socket, seq_num, &SqlError::from(e)).await?;
        }
    }
    Ok(())
}

async fn handle_kill(
    socket: &mut TcpStream,
    _executor: &Arc<Executor>,
    seq_num: u8,
    payload: &[u8],
) -> Result<()> {
    if payload.len() < 5 {
        send_sql_error(
            socket,
            seq_num,
            &SqlError::Parse("KILL requires a connection ID".to_string()),
        )
        .await?;
        return Ok(());
    }

    let connection_id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);

    info!(
        "KILL command: terminating connection {} (current connection tracking not implemented)",
        connection_id
    );

    send_ok_packet(socket, seq_num, "").await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_handshake_response_with_username() {
        let mut payload = vec![0u8; 64];
        payload[0..4].copy_from_slice(&0x000F_7FFF_u32.to_le_bytes());
        payload[4] = 33;
        b"root"
            .iter()
            .enumerate()
            .for_each(|(i, &b)| payload[32 + i] = b);

        let (username, _password) = parse_handshake_response(&payload);
        assert_eq!(username, "root");
    }

    #[test]
    fn test_parse_handshake_response_with_password() {
        let mut payload = vec![0u8; 64];
        payload[0..4].copy_from_slice(&0x000F_7FFF_u32.to_le_bytes());
        payload[4] = 33;
        b"root"
            .iter()
            .enumerate()
            .for_each(|(i, &b)| payload[32 + i] = b);
        payload[36] = 0;
        let password = b"12345678901234567890";
        password
            .iter()
            .enumerate()
            .for_each(|(i, &b)| payload[37 + i] = b);

        let (username, password) = parse_handshake_response(&payload);
        assert_eq!(username, "root");
        assert!(password.is_some());
        assert_eq!(password.unwrap().len(), 20);
    }

    #[test]
    fn test_parse_handshake_response_short_payload() {
        let payload = vec![0u8; 10];

        let (username, _password) = parse_handshake_response(&payload);
        assert_eq!(username, "root");
    }

    #[test]
    fn test_parse_handshake_response_empty_username() {
        let mut payload = vec![0u8; 64];
        payload[0..4].copy_from_slice(&0x000F_7FFF_u32.to_le_bytes());
        payload[4] = 33;
        payload[32] = 0;

        let (username, _password) = parse_handshake_response(&payload);
        assert_eq!(username, "");
    }
}
