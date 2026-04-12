use crate::engines::redis::resp::RespValue;
use crate::engines::redis::to_squeal;
use crate::squeal::exec::{Executor, Session};
use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpStream;

pub async fn hset(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_hset(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    RespValue::Integer(result.rows_affected as i64)
        .write(socket)
        .await
}

pub async fn hget(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_hget(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    if let Some(row) = result.rows.first() {
        if let Some(val) = row.first() {
            RespValue::BulkString(Some(format!("{:?}", val).into_bytes()))
                .write(socket)
                .await?
        } else {
            RespValue::BulkString(None).write(socket).await?
        }
    } else {
        RespValue::BulkString(None).write(socket).await?
    }
    Ok(())
}

pub async fn hgetall(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_hgetall(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    let mut resp_result = vec![];
    for row in &result.rows {
        if row.len() >= 2 {
            let field = &row[0];
            let value = &row[1];
            resp_result.push(RespValue::BulkString(Some(
                format!("{:?}", field).into_bytes(),
            )));
            resp_result.push(RespValue::BulkString(Some(
                format!("{:?}", value).into_bytes(),
            )));
        }
    }
    RespValue::Array(Some(resp_result)).write(socket).await
}

pub async fn hdel(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_hdel(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    RespValue::Integer(result.rows_affected as i64)
        .write(socket)
        .await
}
