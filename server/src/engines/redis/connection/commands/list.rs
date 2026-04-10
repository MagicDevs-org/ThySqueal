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

pub async fn push(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
    cmd_name: &str,
) -> Result<()> {
    require_args(cmd_array, 3, "lpush/rpush")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let values: Vec<crate::storage::Value> = cmd_array[2..]
        .iter()
        .map(|v| extract_value(v).unwrap_or(crate::storage::Value::Null))
        .collect();
    let left = cmd_name == "LPUSH";
    let count = executor.kv_list_push(key, values, left, None).await?;
    RespValue::Integer(count as i64).write(socket).await
}

pub async fn range(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 4, "lrange")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let start = extract_integer(&cmd_array[2])? as i64;
    let stop = extract_integer(&cmd_array[3])? as i64;
    let values = executor.kv_list_range(&key, start, stop, None).await?;
    let result: Vec<RespValue> = values
        .into_iter()
        .map(|v| RespValue::BulkString(Some(format!("{:?}", v).into_bytes())))
        .collect();
    RespValue::Array(Some(result)).write(socket).await
}

pub async fn pop(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
    cmd_name: &str,
) -> Result<()> {
    require_args(cmd_array, 2, "lpop/rpop")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let count = if cmd_array.len() > 2 {
        extract_integer(&cmd_array[2])? as usize
    } else {
        1
    };
    let left = cmd_name == "LPOP";
    let values = executor.kv_list_pop(key, count, left, None).await?;
    let result: Vec<RespValue> = values
        .into_iter()
        .map(|v| RespValue::BulkString(Some(format!("{:?}", v).into_bytes())))
        .collect();
    RespValue::Array(Some(result)).write(socket).await
}

pub async fn len(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "llen")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let len = executor.kv_list_len(&key, None).await?;
    RespValue::Integer(len as i64).write(socket).await
}
