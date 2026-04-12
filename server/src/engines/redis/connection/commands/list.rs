use crate::engines::redis::resp::RespValue;
use crate::engines::redis::to_squeal;
use crate::squeal::exec::{Executor, Session};
use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpStream;

pub async fn push(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
    cmd_name: &str,
) -> Result<()> {
    let squeal = if cmd_name == "LPUSH" {
        to_squeal::parse_lpush(cmd_array)?
    } else {
        to_squeal::parse_rpush(cmd_array)?
    };
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
    let squeal = to_squeal::parse_lrange(cmd_array)?;
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

pub async fn pop(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
    cmd_name: &str,
) -> Result<()> {
    let key = match &cmd_array.get(1) {
        Some(RespValue::BulkString(Some(b))) => String::from_utf8_lossy(b).to_string(),
        Some(RespValue::SimpleString(s)) => s.clone(),
        _ => return Err(anyhow::anyhow!("expected bulk string")),
    };
    let count = if cmd_array.len() > 2 {
        match &cmd_array[2] {
            RespValue::Integer(i) => *i as usize,
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).parse().unwrap_or(1),
            _ => 1,
        }
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
    let key = match &cmd_array.get(1) {
        Some(RespValue::BulkString(Some(b))) => String::from_utf8_lossy(b).to_string(),
        Some(RespValue::SimpleString(s)) => s.clone(),
        _ => return Err(anyhow::anyhow!("expected bulk string")),
    };
    let len = executor.kv_list_len(&key, None).await?;
    RespValue::Integer(len as i64).write(socket).await
}
