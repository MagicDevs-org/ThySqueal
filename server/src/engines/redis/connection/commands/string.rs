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

fn extract_integer(v: &RespValue) -> Result<i64> {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b)
            .parse()
            .map_err(|e| anyhow!("{}", e)),
        RespValue::SimpleString(s) => s.parse().map_err(|e| anyhow!("{}", e)),
        RespValue::Integer(i) => Ok(*i),
        _ => Err(anyhow!("expected integer")),
    }
}

pub async fn ping(socket: &mut TcpStream) -> Result<()> {
    RespValue::SimpleString("PONG".to_string())
        .write(socket)
        .await
}

pub async fn ok_response(socket: &mut TcpStream) -> Result<()> {
    RespValue::SimpleString("OK".to_string())
        .write(socket)
        .await
}

pub async fn set(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 3, "set")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let value = extract_value(&cmd_array[2])?;
    executor.kv_set(key, value, None).await?;
    ok_response(socket).await
}

pub async fn get(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "get")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    match executor.kv_get(&key, None).await? {
        Some(crate::storage::Value::Text(t)) => {
            RespValue::BulkString(Some(t.into_bytes()))
                .write(socket)
                .await?
        }
        Some(v) => {
            RespValue::BulkString(Some(format!("{:?}", v).into_bytes()))
                .write(socket)
                .await?
        }
        None => RespValue::BulkString(None).write(socket).await?,
    }
    Ok(())
}

pub async fn del(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "del")?;
    let mut count = 0;
    for item in cmd_array.iter().skip(1) {
        if let Ok(key) = extract_bulk_string(item)
            && executor.kv_get(&key, None).await?.is_some()
        {
            executor.kv_del(key, None).await?;
            count += 1;
        }
    }
    RespValue::Integer(count).write(socket).await
}

pub async fn exists(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "exists")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let exists = executor.kv_exists(&key, None).await?;
    RespValue::Integer(if exists { 1 } else { 0 })
        .write(socket)
        .await
}

pub async fn expire(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 3, "expire")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let seconds = extract_integer(&cmd_array[2])? as u64;
    let result = executor.kv_expire(key, seconds, None).await?;
    RespValue::Integer(if result { 1 } else { 0 })
        .write(socket)
        .await
}

pub async fn ttl(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "ttl")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let ttl = executor.kv_ttl(&key, None).await?;
    RespValue::Integer(ttl).write(socket).await
}

pub async fn keys(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "keys")?;
    let pattern = extract_bulk_string(&cmd_array[1])?;
    let keys = executor.kv_keys(&pattern, None).await?;
    let result: Vec<RespValue> = keys
        .into_iter()
        .map(|k| RespValue::BulkString(Some(k.into_bytes())))
        .collect();
    RespValue::Array(Some(result)).write(socket).await
}
