use crate::engines::redis::resp::RespValue;
use crate::squeal::exec::Executor;
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

fn extract_value(v: &RespValue) -> Result<crate::storage::Value> {
    match v {
        RespValue::BulkString(Some(b)) => Ok(crate::storage::Value::Text(
            String::from_utf8_lossy(b).to_string(),
        )),
        RespValue::SimpleString(s) => Ok(crate::storage::Value::Text(s.clone())),
        RespValue::Integer(i) => Ok(crate::storage::Value::Int(*i)),
        _ => Err(anyhow!("invalid value type")),
    }
}

pub async fn hset(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 4, "hset")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let field = extract_bulk_string(&cmd_array[2])?;
    let value = extract_value(&cmd_array[3])?;
    executor.kv_hash_set(key, field, value, None).await?;
    RespValue::Integer(1).write(socket).await
}

pub async fn hget(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 3, "hget")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let field = extract_bulk_string(&cmd_array[2])?;
    match executor.kv_hash_get(&key, &field, None).await? {
        Some(v) => {
            RespValue::BulkString(Some(format!("{:?}", v).into_bytes()))
                .write(socket)
                .await?
        }
        None => RespValue::BulkString(None).write(socket).await?,
    }
    Ok(())
}

pub async fn hgetall(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "hgetall")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let hash = executor.kv_hash_get_all(&key, None).await?;
    let mut result = vec![];
    for (field, value) in hash {
        result.push(RespValue::BulkString(Some(field.into_bytes())));
        result.push(RespValue::BulkString(Some(
            format!("{:?}", value).into_bytes(),
        )));
    }
    RespValue::Array(Some(result)).write(socket).await
}

pub async fn hdel(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 3, "hdel")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let fields: Vec<String> = cmd_array[2..]
        .iter()
        .filter_map(|v| extract_bulk_string(v).ok())
        .collect();
    let count = executor.kv_hash_del(key, fields, None).await?;
    RespValue::Integer(count as i64).write(socket).await
}
