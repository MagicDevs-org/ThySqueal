use crate::engines::redis::resp::RespValue;
use crate::squeal::ir::{
    KvDel, KvGet, KvHashGet, KvHashSet, KvListPush, KvListRange, KvSet, KvSetAdd, KvSetMembers,
    KvStreamAdd, KvStreamLen, KvStreamRange, KvZSetAdd, KvZSetRange, PubSubPublish, Squeal,
};
use crate::storage::Value;
use anyhow::{Result, anyhow};

pub fn parse_set(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 3 {
        return Err(anyhow!("wrong number of arguments for 'set' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let value = extract_value(&cmd_array[2])?;

    let mut expiry = None;
    if cmd_array.len() > 3 {
        let opt = extract_bulk_string(&cmd_array[3])?;
        match opt.to_uppercase().as_str() {
            "EX" => {
                let secs: i64 = extract_integer(&cmd_array[4])?;
                expiry = Some(secs as u64);
            }
            "PX" => {
                let ms: i64 = extract_integer(&cmd_array[4])?;
                expiry = Some(ms as u64 / 1000);
            }
            "EXAT" => {
                let ts: i64 = extract_integer(&cmd_array[4])?;
                expiry = Some(ts as u64);
            }
            "PXAT" => {
                let ts: i64 = extract_integer(&cmd_array[4])?;
                expiry = Some(ts as u64 / 1000);
            }
            _ => {}
        }
    }

    Ok(Squeal::KvSet(KvSet { key, value, expiry }))
}

pub fn parse_get(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 2 {
        return Err(anyhow!("wrong number of arguments for 'get' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    Ok(Squeal::KvGet(KvGet { key }))
}

pub fn parse_del(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 2 {
        return Err(anyhow!("wrong number of arguments for 'del' command"));
    }
    let mut keys = Vec::new();
    for item in cmd_array.iter().skip(1) {
        keys.push(extract_bulk_string(item)?);
    }
    Ok(Squeal::KvDel(KvDel { keys }))
}

pub fn parse_exists(cmd_array: &[RespValue]) -> Result<Squeal> {
    parse_get(cmd_array)
}

pub fn parse_expire(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 3 {
        return Err(anyhow!("wrong number of arguments for 'expire' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let seconds = extract_integer(&cmd_array[2])? as u64;
    Ok(Squeal::KvSet(KvSet {
        key,
        value: Value::Null,
        expiry: Some(seconds),
    }))
}

#[allow(dead_code)]
pub fn parse_ttl(cmd_array: &[RespValue]) -> Result<Squeal> {
    parse_get(cmd_array)
}

pub fn parse_keys(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 2 {
        return Err(anyhow!("wrong number of arguments for 'keys' command"));
    }
    let pattern = extract_bulk_string(&cmd_array[1])?;
    Ok(Squeal::KvGet(KvGet { key: pattern }))
}

pub fn parse_hset(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 4 {
        return Err(anyhow!("wrong number of arguments for 'hset' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let field = extract_bulk_string(&cmd_array[2])?;
    let value = extract_value(&cmd_array[3])?;
    Ok(Squeal::KvHashSet(KvHashSet { key, field, value }))
}

pub fn parse_hget(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 3 {
        return Err(anyhow!("wrong number of arguments for 'hget' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let field = extract_bulk_string(&cmd_array[2])?;
    Ok(Squeal::KvHashGet(KvHashGet { key, field }))
}

pub fn parse_hgetall(cmd_array: &[RespValue]) -> Result<Squeal> {
    parse_hget(cmd_array)
}

pub fn parse_hdel(cmd_array: &[RespValue]) -> Result<Squeal> {
    parse_del(cmd_array)
}

pub fn parse_lpush(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 3 {
        return Err(anyhow!("wrong number of arguments for 'lpush' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let mut values = Vec::new();
    for item in cmd_array.iter().skip(2) {
        values.push(extract_value(item)?);
    }
    Ok(Squeal::KvListPush(KvListPush {
        key,
        values,
        left: true,
    }))
}

pub fn parse_rpush(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 3 {
        return Err(anyhow!("wrong number of arguments for 'rpush' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let mut values = Vec::new();
    for item in cmd_array.iter().skip(2) {
        values.push(extract_value(item)?);
    }
    Ok(Squeal::KvListPush(KvListPush {
        key,
        values,
        left: false,
    }))
}

pub fn parse_lrange(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 4 {
        return Err(anyhow!("wrong number of arguments for 'lrange' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let start = extract_integer(&cmd_array[2])?;
    let stop = extract_integer(&cmd_array[3])?;
    Ok(Squeal::KvListRange(KvListRange { key, start, stop }))
}

pub fn parse_sadd(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 3 {
        return Err(anyhow!("wrong number of arguments for 'sadd' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let mut members = Vec::new();
    for item in cmd_array.iter().skip(2) {
        members.push(extract_bulk_string(item)?);
    }
    Ok(Squeal::KvSetAdd(KvSetAdd { key, members }))
}

pub fn parse_smembers(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 2 {
        return Err(anyhow!("wrong number of arguments for 'smembers' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    Ok(Squeal::KvSetMembers(KvSetMembers { key }))
}

pub fn parse_zadd(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 4 {
        return Err(anyhow!("wrong number of arguments for 'zadd' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let score = extract_float(&cmd_array[2])?;
    let member = extract_bulk_string(&cmd_array[3])?;
    Ok(Squeal::KvZSetAdd(KvZSetAdd {
        key,
        members: vec![(score, member)],
    }))
}

pub fn parse_zrange(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 4 {
        return Err(anyhow!("wrong number of arguments for 'zrange' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let start = extract_integer(&cmd_array[2])?;
    let stop = extract_integer(&cmd_array[3])?;
    let with_scores = cmd_array.len() > 4
        && extract_bulk_string(&cmd_array[4])
            .ok()
            .map(|s| s.to_uppercase())
            == Some("WITHSCORES".to_string());
    Ok(Squeal::KvZSetRange(KvZSetRange {
        key,
        start,
        stop,
        with_scores,
    }))
}

pub fn parse_xadd(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 4 {
        return Err(anyhow!("wrong number of arguments for 'xadd' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let mut fields = std::collections::HashMap::new();
    for i in (2..cmd_array.len()).step_by(2) {
        let field = extract_bulk_string(&cmd_array[i])?;
        let value = extract_value(&cmd_array[i + 1])?;
        fields.insert(field, value);
    }
    Ok(Squeal::KvStreamAdd(KvStreamAdd {
        key,
        id: None,
        fields,
    }))
}

pub fn parse_xrange(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 4 {
        return Err(anyhow!("wrong number of arguments for 'xrange' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    let start = extract_bulk_string(&cmd_array[2])?;
    let stop = extract_bulk_string(&cmd_array[3])?;
    Ok(Squeal::KvStreamRange(KvStreamRange {
        key,
        start,
        stop,
        count: None,
    }))
}

pub fn parse_xlen(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 2 {
        return Err(anyhow!("wrong number of arguments for 'xlen' command"));
    }
    let key = extract_bulk_string(&cmd_array[1])?;
    Ok(Squeal::KvStreamLen(KvStreamLen { key }))
}

#[allow(dead_code)]
pub fn parse_publish(cmd_array: &[RespValue]) -> Result<Squeal> {
    if cmd_array.len() < 3 {
        return Err(anyhow!("wrong number of arguments for 'publish' command"));
    }
    let channel = extract_bulk_string(&cmd_array[1])?;
    let message = extract_bulk_string(&cmd_array[2])?;
    Ok(Squeal::PubSubPublish(PubSubPublish { channel, message }))
}

fn extract_bulk_string(v: &RespValue) -> Result<String> {
    match v {
        RespValue::BulkString(Some(b)) => Ok(String::from_utf8_lossy(b).to_string()),
        RespValue::SimpleString(s) => Ok(s.clone()),
        _ => Err(anyhow!("expected bulk string")),
    }
}

fn extract_value(v: &RespValue) -> Result<Value> {
    match v {
        RespValue::BulkString(Some(b)) => Ok(Value::Text(String::from_utf8_lossy(b).to_string())),
        RespValue::SimpleString(s) => Ok(Value::Text(s.clone())),
        RespValue::Integer(i) => Ok(Value::Int(*i)),
        RespValue::BulkString(None) => Ok(Value::Null),
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

fn extract_float(v: &RespValue) -> Result<f64> {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b)
            .parse()
            .map_err(|e| anyhow!("{}", e)),
        RespValue::SimpleString(s) => s.parse().map_err(|e| anyhow!("{}", e)),
        RespValue::Integer(i) => Ok(*i as f64),
        _ => Err(anyhow!("expected float")),
    }
}
