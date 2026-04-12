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
    let squeal = to_squeal::parse_sadd(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    RespValue::Integer(result.rows_affected as i64)
        .write(socket)
        .await
}

pub async fn members(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_smembers(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    let members: Vec<RespValue> = result
        .rows
        .iter()
        .filter_map(|row| row.first())
        .filter_map(|v| match v {
            crate::storage::Value::Text(t) => {
                Some(RespValue::BulkString(Some(t.as_bytes().to_vec())))
            }
            _ => None,
        })
        .collect();
    RespValue::Array(Some(members)).write(socket).await
}

pub async fn is_member(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let key = match &cmd_array.get(1) {
        Some(RespValue::BulkString(Some(b))) => String::from_utf8_lossy(b).to_string(),
        Some(RespValue::SimpleString(s)) => s.clone(),
        _ => return Err(anyhow::anyhow!("expected bulk string")),
    };
    let member = match &cmd_array.get(2) {
        Some(RespValue::BulkString(Some(b))) => String::from_utf8_lossy(b).to_string(),
        Some(RespValue::SimpleString(s)) => s.clone(),
        _ => return Err(anyhow::anyhow!("expected bulk string")),
    };
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
    let count = executor.kv_set_remove(key, members, None).await?;
    RespValue::Integer(count as i64).write(socket).await
}
