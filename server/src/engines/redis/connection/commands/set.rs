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

pub async fn add(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 3, "sadd")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let members: Vec<String> = cmd_array[2..]
        .iter()
        .filter_map(|v| extract_bulk_string(v).ok())
        .collect();
    let count = executor.kv_set_add(key, members, None).await?;
    RespValue::Integer(count as i64).write(socket).await
}

pub async fn members(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "smembers")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let members = executor.kv_set_members(&key, None).await?;
    let result: Vec<RespValue> = members
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m.into_bytes())))
        .collect();
    RespValue::Array(Some(result)).write(socket).await
}

pub async fn is_member(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 3, "sismember")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let member = extract_bulk_string(&cmd_array[2])?;
    let exists = executor.kv_set_is_member(&key, &member, None).await?;
    RespValue::Integer(if exists { 1 } else { 0 })
        .write(socket)
        .await
}

pub async fn remove(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 3, "srem")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let members: Vec<String> = cmd_array[2..]
        .iter()
        .filter_map(|v| extract_bulk_string(v).ok())
        .collect();
    let count = executor.kv_set_remove(key, members, None).await?;
    RespValue::Integer(count as i64).write(socket).await
}
