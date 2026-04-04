use crate::redis::resp::RespValue;
use crate::sql::executor::Executor;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
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

pub async fn add(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 4, "xadd")?;
    let key = extract_bulk_string(&cmd_array[1])?;

    let id = if matches!(&cmd_array[2], RespValue::BulkString(Some(b)) if b.starts_with(b"*")) {
        None
    } else if cmd_array.len() > 2 {
        Some(extract_integer(&cmd_array[2])? as u64)
    } else {
        None
    };

    let start_idx = if cmd_array.len() > 2
        && matches!(&cmd_array[2], RespValue::BulkString(Some(b)) if !b.starts_with(b"*"))
    {
        3
    } else {
        2
    };

    let mut fields = HashMap::new();
    let mut i = start_idx;
    while i + 1 < cmd_array.len() {
        let field = extract_bulk_string(&cmd_array[i])?;
        let value =
            extract_value(&cmd_array[i + 1]).unwrap_or(crate::storage::Value::Text("".to_string()));
        fields.insert(field, value);
        i += 2;
    }

    let stream_id = executor.kv_stream_add(key, id, fields, None).await?;
    RespValue::BulkString(Some(stream_id.into_bytes()))
        .write(socket)
        .await
}

pub async fn range(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 4, "xrange")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let start = extract_bulk_string(&cmd_array[2])?;
    let stop = extract_bulk_string(&cmd_array[3])?;

    let count = if cmd_array.len() > 4
        && matches!(&cmd_array[4], RespValue::BulkString(Some(b)) if b == b"COUNT")
    {
        Some(extract_integer(&cmd_array[5])? as usize)
    } else {
        None
    };

    let results = executor
        .kv_stream_range(&key, &start, &stop, count, None)
        .await?;

    let mut result = vec![];
    for (id, fields) in results {
        let mut entry = vec![RespValue::BulkString(Some(id.into_bytes()))];
        let mut field_values = vec![];
        for (field, value) in fields {
            field_values.push(RespValue::BulkString(Some(field.into_bytes())));
            field_values.push(RespValue::BulkString(Some(
                format!("{:?}", value).into_bytes(),
            )));
        }
        entry.push(RespValue::Array(Some(field_values)));
        result.push(RespValue::Array(Some(entry)));
    }

    RespValue::Array(Some(result)).write(socket).await
}

pub async fn len(
    socket: &mut TcpStream,
    cmd_array: &[RespValue],
    executor: &Arc<Executor>,
) -> Result<()> {
    require_args(cmd_array, 2, "xlen")?;
    let key = extract_bulk_string(&cmd_array[1])?;
    let len = executor.kv_stream_len(&key, None).await?;
    RespValue::Integer(len as i64).write(socket).await
}
