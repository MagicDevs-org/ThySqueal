use crate::engines::redis::resp::RespValue;
use crate::engines::redis::to_squeal;
use crate::squeal::exec::{Executor, Session};
use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpStream;

pub async fn add(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_zadd(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    RespValue::Integer(result.rows_affected as i64)
        .write(socket)
        .await
}

pub async fn range(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_zrange(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    let values: Vec<RespValue> = result
        .rows
        .iter()
        .filter_map(|row| row.first())
        .map(|v| RespValue::BulkString(Some(format!("{:?}", v).into_bytes())))
        .collect();
    RespValue::Array(Some(values)).write(socket).await
}

pub async fn rangebyscore(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let key = match &cmd_array.get(1) {
        Some(RespValue::BulkString(Some(b))) => String::from_utf8_lossy(b).to_string(),
        Some(RespValue::SimpleString(s)) => s.clone(),
        _ => return Err(anyhow::anyhow!("expected bulk string")),
    };
    let min = match &cmd_array.get(2) {
        Some(RespValue::BulkString(Some(b))) => {
            String::from_utf8_lossy(b).parse().unwrap_or(f64::MIN)
        }
        Some(RespValue::Integer(i)) => *i as f64,
        _ => f64::MIN,
    };
    let max = match &cmd_array.get(3) {
        Some(RespValue::BulkString(Some(b))) => {
            String::from_utf8_lossy(b).parse().unwrap_or(f64::MAX)
        }
        Some(RespValue::Integer(i)) => *i as f64,
        _ => f64::MAX,
    };
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
    let key = match &cmd_array.get(1) {
        Some(RespValue::BulkString(Some(b))) => String::from_utf8_lossy(b).to_string(),
        Some(RespValue::SimpleString(s)) => s.clone(),
        _ => return Err(anyhow::anyhow!("expected bulk string")),
    };
    let members: Vec<String> = cmd_array[2..]
        .iter()
        .filter_map(|v| match v {
            RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
            RespValue::SimpleString(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    let count = executor.kv_zset_remove(key, members, None).await?;
    RespValue::Integer(count as i64).write(socket).await
}
