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

fn extract_float(v: &RespValue) -> Result<f64> {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b)
            .parse()
            .map_err(|e| anyhow!("{}", e)),
        RespValue::SimpleString(s) => s.parse().map_err(|e| anyhow!("{}", e)),
        RespValue::Integer(i) => Ok(*i as f64),
        _ => Err(anyhow!("expected number")),
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

pub async fn add(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 4, "zadd")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let mut members = vec![];
    let mut i = 2;
    while i + 1 < cmd_array.len() {
        let score = extract_float(&cmd_array[i])?;
        let member = extract_bulk_string(&cmd_array[i + 1])?;
        members.push((score, member));
        i += 2;
    }
    let count = executor.kv_zset_add(key, members, None).await?;
    RespValue::Integer(count as i64).write(socket).await
}

pub async fn range(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 4, "zrange")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let start = extract_integer(&cmd_array[2])?;
    let stop = extract_integer(&cmd_array[3])?;
    let with_scores = cmd_array.len() > 4
        && matches!(&cmd_array[4], RespValue::BulkString(Some(b)) if b == b"WITHSCORES");
    let values = executor
        .kv_zset_range(&key, start, stop, with_scores, None)
        .await?;
    let result: Vec<RespValue> = values
        .into_iter()
        .map(|v| RespValue::BulkString(Some(format!("{:?}", v).into_bytes())))
        .collect();
    RespValue::Array(Some(result)).write(socket).await
}

pub async fn rangebyscore(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 4, "zrangebyscore")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let min = extract_float(&cmd_array[2])?;
    let max = extract_float(&cmd_array[3])?;
    let with_scores = cmd_array.len() > 4
        && matches!(&cmd_array[4], RespValue::BulkString(Some(b)) if b == b"WITHSCORES");
    let values = executor
        .kv_zsetrangebyscore(&key, min, max, with_scores, None)
        .await?;
    let result: Vec<RespValue> = values
        .into_iter()
        .map(|v| RespValue::BulkString(Some(format!("{:?}", v).into_bytes())))
        .collect();
    RespValue::Array(Some(result)).write(socket).await
}

pub async fn remove(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 3, "zrem")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let members: Vec<String> = cmd_array[2..]
        .iter()
        .filter_map(|v| extract_bulk_string(v).ok())
        .collect();
    let count = executor.kv_zset_remove(key, members, None).await?;
    RespValue::Integer(count as i64).write(socket).await
}
