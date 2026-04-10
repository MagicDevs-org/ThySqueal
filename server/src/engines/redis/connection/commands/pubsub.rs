use crate::engines::redis::resp::RespValue;
use crate::engines_mysql::executor::Executor;
use anyhow::{Result, anyhow};
use std::sync::Arc;
use tokio::net::TcpStream;

fn require_args(cmd_array: &[RespValue], min_len: usize, name: &str) -> Result<()> {
    if cmd_array.len() < min_len {
        return Err(anyhow!("wrong number of arguments for '{}' command", name));
    }
    Ok(())
}

fn extract_bulk_string(v: &RespValue) -> Result<String> {
    match v {
        RespValue::BulkString(Some(b)) => Ok(String::from_utf8_lossy(b).to_string()),
        RespValue::SimpleString(s) => Ok(s.clone()),
        _ => Err(anyhow!("expected bulk string")),
    }
}

pub async fn publish(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 3, "publish")?;
    let channel = extract_bulk_string(&cmd_array[1])?;
    let message = extract_bulk_string(&cmd_array[2])?;
    let count = executor.pubsub_publish(channel, message).await?;
    RespValue::Integer(count as i64).write(socket).await
}

pub async fn subscribe(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "subscribe")?;
    let client_id = format!("{:p}", socket);
    for item in cmd_array.iter().skip(1) {
        let channel = extract_bulk_string(item)?;
        executor
            .pubsub_subscribe(client_id.clone(), channel.clone())
            .await?;
        RespValue::Array(Some(vec![
            RespValue::SimpleString("subscribe".to_string()),
            RespValue::BulkString(Some(channel.into_bytes())),
            RespValue::Integer(1),
        ]))
        .write(socket)
        .await?;
    }
    Ok(())
}

pub async fn unsubscribe(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let client_id = format!("{:p}", socket);
    if cmd_array.len() < 2 {
        executor.pubsub_unsubscribe(client_id, None).await?;
    } else {
        for item in cmd_array.iter().skip(1) {
            let channel = extract_bulk_string(item)?;
            executor
                .pubsub_unsubscribe(client_id.clone(), Some(channel))
                .await?;
        }
    }
    RespValue::Array(Some(vec![
        RespValue::SimpleString("unsubscribe".to_string()),
        RespValue::BulkString(None),
        RespValue::Integer(0),
    ]))
    .write(socket)
    .await
}

pub async fn pubsub(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "pubsub")?;
    let subcommand = extract_bulk_string(&cmd_array[1])?.to_uppercase();
    match subcommand.as_str() {
        "CHANNELS" => {
            let channels = executor.pubsub_channels().await?;
            let result: Vec<RespValue> = channels
                .into_iter()
                .map(|c| RespValue::BulkString(Some(c.into_bytes())))
                .collect();
            RespValue::Array(Some(result)).write(socket).await
        }
        "NUMSUB" => RespValue::Integer(0).write(socket).await,
        _ => {
            RespValue::Error("ERR Unknown PUBSUB subcommand".to_string())
                .write(socket)
                .await
        }
    }
}
