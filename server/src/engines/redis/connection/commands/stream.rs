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
    let squeal = to_squeal::parse_xadd(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    if let Some(row) = result.rows.first() {
        if let Some(id) = row.first() {
            RespValue::BulkString(Some(format!("{:?}", id).into_bytes()))
                .write(socket)
                .await
        } else {
            RespValue::BulkString(None).write(socket).await
        }
    } else {
        RespValue::BulkString(None).write(socket).await
    }
}

pub async fn range(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_xrange(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    let mut resp_result = vec![];
    for row in &result.rows {
        if row.len() >= 2 {
            let id = &row[0];
            let fields = &row[1];
            let mut entry = vec![RespValue::BulkString(Some(
                format!("{:?}", id).into_bytes(),
            ))];
            if let crate::storage::Value::Text(t) = fields {
                entry.push(RespValue::BulkString(Some(t.as_bytes().to_vec())));
            }
            resp_result.push(RespValue::Array(Some(entry)));
        }
    }
    RespValue::Array(Some(resp_result)).write(socket).await
}

pub async fn len(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    let squeal = to_squeal::parse_xlen(cmd_array)?;
    let result = executor
        .execute_squeal(squeal, vec![], Session::root())
        .await?;
    let len = result
        .rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| match v {
            crate::storage::Value::Int(i) => Some(*i),
            _ => None,
        })
        .unwrap_or(0);
    RespValue::Integer(len).write(socket).await
}
