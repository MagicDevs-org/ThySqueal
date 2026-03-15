use super::resp::{RespValue, read_value};
use crate::sql::executor::Executor;
use crate::storage::Value;
use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::{debug, error};

pub async fn handle_connection(mut socket: TcpStream, executor: Arc<Executor>) -> Result<()> {
    loop {
        let value = match read_value(&mut socket).await {
            Ok(v) => v,
            Err(e) => {
                if e.to_string().contains("early eof") || e.to_string().contains("broken pipe") {
                    break;
                }
                error!("Error reading RESP value: {}", e);
                break;
            }
        };

        let cmd_array = match value {
            RespValue::Array(Some(a)) => a,
            _ => {
                RespValue::Error("ERR expected array".to_string())
                    .write(&mut socket)
                    .await?;
                continue;
            }
        };

        if cmd_array.is_empty() {
            continue;
        }

        let cmd_name = match &cmd_array[0] {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_uppercase(),
            RespValue::SimpleString(s) => s.to_uppercase(),
            _ => {
                RespValue::Error("ERR invalid command name type".to_string())
                    .write(&mut socket)
                    .await?;
                continue;
            }
        };

        debug!("Redis command: {}", cmd_name);

        match cmd_name.as_str() {
            "PING" => {
                RespValue::SimpleString("PONG".to_string())
                    .write(&mut socket)
                    .await?;
            }
            "SET" => {
                if cmd_array.len() < 3 {
                    RespValue::Error("ERR wrong number of arguments for 'set' command".to_string())
                        .write(&mut socket)
                        .await?;
                    continue;
                }
                let key = match &cmd_array[1] {
                    RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        RespValue::Error("ERR invalid key type".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };
                let val = match &cmd_array[2] {
                    RespValue::BulkString(Some(b)) => {
                        Value::Text(String::from_utf8_lossy(b).to_string())
                    }
                    _ => {
                        RespValue::Error("ERR invalid value type".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };

                executor.kv_set(key, val, None).await?;
                RespValue::SimpleString("OK".to_string())
                    .write(&mut socket)
                    .await?;
            }
            "GET" => {
                if cmd_array.len() < 2 {
                    RespValue::Error("ERR wrong number of arguments for 'get' command".to_string())
                        .write(&mut socket)
                        .await?;
                    continue;
                }
                let key = match &cmd_array[1] {
                    RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        RespValue::Error("ERR invalid key type".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };

                match executor.kv_get(&key, None).await? {
                    Some(Value::Text(t)) => {
                        RespValue::BulkString(Some(t.into_bytes()))
                            .write(&mut socket)
                            .await?;
                    }
                    Some(v) => {
                        RespValue::BulkString(Some(format!("{:?}", v).into_bytes()))
                            .write(&mut socket)
                            .await?;
                    }
                    None => {
                        RespValue::BulkString(None).write(&mut socket).await?;
                    }
                }
            }
            "DEL" => {
                if cmd_array.len() < 2 {
                    RespValue::Error("ERR wrong number of arguments for 'del' command".to_string())
                        .write(&mut socket)
                        .await?;
                    continue;
                }
                let mut count = 0;
                for item in cmd_array {
                    let key = match item {
                        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(),
                        _ => continue,
                    };
                    if executor.kv_get(&key, None).await?.is_some() {
                        executor.kv_del(key, None).await?;
                        count += 1;
                    }
                }
                RespValue::Integer(count).write(&mut socket).await?;
            }
            "QUIT" => {
                RespValue::SimpleString("OK".to_string())
                    .write(&mut socket)
                    .await?;
                break;
            }
            _ => {
                RespValue::Error(format!("ERR unknown command '{}'", cmd_name))
                    .write(&mut socket)
                    .await?;
            }
        }
    }
    Ok(())
}
