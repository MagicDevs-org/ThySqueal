use crate::engines::redis::resp::RespValue;
use crate::engines::redis::to_squeal;
use crate::squeal::exec::{Executor, Session};
use crate::squeal::ir::Squeal;
use anyhow::{Result, anyhow};
use std::sync::Arc;
use tokio::net::TcpStream;

fn require_args(cmd_array: &[RespValue], min_len: usize, name: &str) -> Result<()> {
    if cmd_array.len() < min_len {
        return Err(anyhow!("wrong number of arguments for '{}' command", name));
    }
    Ok(())
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
    let squeal = to_squeal::parse_set(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    if result.rows_affected > 0 {
        ok_response(socket).await
    } else {
        RespValue::Error("ERR failed to set key".to_string())
            .write(socket)
            .await
    }
}

pub async fn get(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "get")?;
    let key = match &cmd_array[1] {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        _ => {
            return Err(anyhow!("expected bulk string"));
        }
    };

    let result = executor
        .execute_squeal(
            Squeal::KvGet(crate::squeal::ir::KvGet { key }),
            vec![],
            Session::root(),
        )
        .await?;

    if let Some(row) = result.rows.first() {
        if let Some(val) = row.first() {
            match val {
                crate::storage::Value::Text(t) => {
                    RespValue::BulkString(Some(t.as_bytes().to_vec()))
                        .write(socket)
                        .await?
                }
                crate::storage::Value::Int(i) => {
                    RespValue::BulkString(Some(i.to_string().into_bytes()))
                        .write(socket)
                        .await?
                }
                _ => {
                    RespValue::BulkString(Some(format!("{:?}", val).into_bytes()))
                        .write(socket)
                        .await?
                }
            }
        } else {
            RespValue::BulkString(None).write(socket).await?
        }
    } else {
        RespValue::BulkString(None).write(socket).await?
    }
    Ok(())
}

pub async fn del(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_del(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    RespValue::Integer(result.rows_affected as i64)
        .write(socket)
        .await
}

pub async fn exists(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_exists(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    let exists = !result.rows.is_empty();
    RespValue::Integer(if exists { 1 } else { 0 })
        .write(socket)
        .await
}

pub async fn expire(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_expire(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    RespValue::Integer(if result.rows_affected > 0 { 1 } else { 0 })
        .write(socket)
        .await
}

pub async fn ttl(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "ttl")?;
    let key = match &cmd_array[1] {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        _ => return Err(anyhow!("expected bulk string")),
    };

    let result = executor.kv_ttl(&key, None).await?;
    RespValue::Integer(result).write(socket).await
}

pub async fn keys(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_keys(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    let keys: Vec<RespValue> = result
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
    RespValue::Array(Some(keys)).write(socket).await
}
