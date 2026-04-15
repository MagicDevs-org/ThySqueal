use std::collections::HashMap;
use tokio::net::TcpStream;

use super::constants::*;
use super::packet::*;
use super::types::*;
use crate::engines::mysql::error::SqlError;
use crate::squeal::exec::{Executor, Session};
use anyhow::Result;
use bcrypt::verify;
use std::sync::Arc;
use tracing::info;

pub async fn handle_connection(mut socket: TcpStream, executor: Arc<Executor>) -> Result<()> {
    send_handshake(&mut socket).await?;

    let (seq, payload) = read_packet(&mut socket).await?;

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
    let _password_hash: Option<Vec<u8>> =
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

    let db = executor.db.read().await;
    let auth_ok = if let Some(user) = db.state().users.get(&username) {
        user.password_hash.is_empty() || verify("", &user.password_hash).unwrap_or(true)
    } else {
        true
    };
    drop(db);

    if auth_ok {
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

    let mut seq_num = seq + 2;
    let mut prepared_statements: HashMap<u64, PreparedStatement> = HashMap::new();
    let mut stmt_id_counter: u64 = 0;

    loop {
        match read_packet(&mut socket).await {
            Ok((seq, payload)) => {
                if payload.is_empty() {
                    continue;
                }

                let cmd = payload[0];
                seq_num = seq + 1;

                match cmd {
                    COM_QUIT => {
                        info!("MySQL client disconnected");
                        break;
                    }
                    COM_PING => {
                        send_ok_packet(&mut socket, seq_num, "PONG").await?;
                    }
                    COM_INIT_DB => {
                        let db_name = String::from_utf8_lossy(&payload[1..]).to_string();
                        let result = executor
                            .execute(
                                &format!("USE {}", db_name),
                                vec![],
                                Session::new(None, None),
                            )
                            .await;
                        match result {
                            Ok(_) => send_ok_packet(&mut socket, seq_num, "").await?,
                            Err(e) => {
                                send_sql_error(&mut socket, seq_num, &SqlError::from(e)).await?
                            }
                        }
                    }
                    COM_QUERY => {
                        let query = String::from_utf8_lossy(&payload[1..]).to_string();
                        let result = executor
                            .execute(&query, vec![], Session::new(None, None))
                            .await;
                        match result {
                            Ok(res) => {
                                send_resultset(&mut socket, seq_num, res).await?;
                            }
                            Err(e) => {
                                send_sql_error(&mut socket, seq_num, &SqlError::from(e)).await?;
                            }
                        }
                    }
                    COM_STMT_PREPARE => {
                        let query = String::from_utf8_lossy(&payload[1..]).to_string();
                        stmt_id_counter += 1;
                        let stmt_id = stmt_id_counter;
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

                        send_stmt_prepare_ok(
                            &mut socket,
                            seq_num,
                            stmt_id,
                            num_columns,
                            num_params,
                        )
                        .await?;
                    }
                    COM_STMT_EXECUTE => {
                        if payload.len() < 5 {
                            send_sql_error(
                                &mut socket,
                                seq_num,
                                &SqlError::Parse("Invalid statement execute".to_string()),
                            )
                            .await?;
                            continue;
                        }

                        let stmt_id =
                            u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                        let stmt = match prepared_statements.get(&(stmt_id as u64)) {
                            Some(s) => s.clone(),
                            None => {
                                send_sql_error(
                                    &mut socket,
                                    seq_num,
                                    &SqlError::Parse(format!("Unknown statement id: {}", stmt_id)),
                                )
                                .await?;
                                continue;
                            }
                        };

                        let null_bitmap_offset = 9;
                        let num_params = stmt.params.len();
                        let bound_params = if num_params > 0 {
                            let null_bitmap_len = (num_params + 7) / 8;
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
                            Ok(res) => send_resultset(&mut socket, seq_num, res).await?,
                            Err(e) => {
                                send_sql_error(&mut socket, seq_num, &SqlError::from(e)).await?
                            }
                        }
                    }
                    COM_STMT_CLOSE => {
                        if payload.len() >= 5 {
                            let stmt_id = u32::from_le_bytes([
                                payload[1], payload[2], payload[3], payload[4],
                            ]);
                            prepared_statements.remove(&(stmt_id as u64));
                        }
                    }
                    COM_CREATE_DB => {
                        let db_name = String::from_utf8_lossy(&payload[1..]).to_string();
                        let result = executor
                            .execute(
                                &format!("CREATE DATABASE {}", db_name),
                                vec![],
                                Session::new(None, None),
                            )
                            .await;
                        match result {
                            Ok(_) => send_ok_packet(&mut socket, seq_num, "").await?,
                            Err(e) => {
                                send_sql_error(&mut socket, seq_num, &SqlError::from(e)).await?
                            }
                        }
                    }
                    COM_DROP_DB => {
                        let db_name = String::from_utf8_lossy(&payload[1..]).to_string();
                        let result = executor
                            .execute(
                                &format!("DROP DATABASE {}", db_name),
                                vec![],
                                Session::new(None, None),
                            )
                            .await;
                        match result {
                            Ok(_) => send_ok_packet(&mut socket, seq_num, "").await?,
                            Err(e) => {
                                send_sql_error(&mut socket, seq_num, &SqlError::from(e)).await?
                            }
                        }
                    }
                    COM_FIELD_LIST => {
                        let table_name = String::from_utf8_lossy(&payload[1..]).to_string();
                        let result = executor
                            .execute(
                                &format!("SHOW COLUMNS FROM {}", table_name),
                                vec![],
                                Session::new(None, None),
                            )
                            .await;
                        match result {
                            Ok(res) => send_resultset(&mut socket, seq_num, res).await?,
                            Err(e) => {
                                send_sql_error(&mut socket, seq_num, &SqlError::from(e)).await?
                            }
                        }
                    }
                    COM_STATISTICS => {
                        send_ok_packet(&mut socket, seq_num, "Threads: 1  Questions: 0  Slow: 0")
                            .await?;
                    }
                    COM_DEBUG | COM_TIME => {
                        send_ok_packet(&mut socket, seq_num, "").await?;
                    }
                    _ => {
                        send_sql_error(
                            &mut socket,
                            seq_num,
                            &SqlError::Parse(format!("Unknown command: {}", cmd)),
                        )
                        .await?;
                    }
                }
            }
            Err(e) => {
                info!("MySQL connection error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
