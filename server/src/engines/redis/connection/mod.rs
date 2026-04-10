mod commands;

use crate::engines::redis::connection::commands::{hash, list, pubsub, set, stream, string, zset};
use crate::engines::redis::resp::{RespValue, read_value};
use crate::squeal::exec::Executor;
use anyhow::{Result, anyhow};
use std::sync::Arc;
use tokio::net::TcpStream;

fn extract_command_name(cmd_array: &[RespValue]) -> Result<String> {
    match &cmd_array[0] {
        RespValue::BulkString(Some(b)) => Ok(String::from_utf8_lossy(b).to_uppercase()),
        RespValue::SimpleString(s) => Ok(s.to_uppercase()),
        _ => Err(anyhow!("invalid command name type")),
    }
}

pub async fn handle_connection(mut socket: TcpStream, executor: Arc<Executor>) -> Result<()> {
    loop {
        let value = match read_value(&mut socket).await {
            Ok(v) => v,
            Err(e)
                if e.to_string().contains("early eof") || e.to_string().contains("broken pipe") =>
            {
                break;
            }
            Err(e) => {
                tracing::error!("Error reading RESP value: {}", e);
                break;
            }
        };

        let cmd_array = match value {
            RespValue::Array(Some(a)) => a,
            _ => {
                RespValue::Error("ERR expected array".to_string())
                    .write(&mut socket)
                    .await?;
                continue;
            }
        };

        if cmd_array.is_empty() {
            continue;
        }

        let cmd_name = extract_command_name(&cmd_array)?;
        tracing::debug!("Redis command: {}", cmd_name);

        match cmd_name.as_str() {
            "PING" => string::ping(&mut socket).await?,
            "QUIT" => {
                string::ok_response(&mut socket).await?;
                break;
            }
            "SET" => string::set(&mut socket, &cmd_array, &executor).await?,
            "GET" => string::get(&mut socket, &cmd_array, &executor).await?,
            "DEL" => string::del(&mut socket, &cmd_array, &executor).await?,
            "EXISTS" => string::exists(&mut socket, &cmd_array, &executor).await?,
            "EXPIRE" => string::expire(&mut socket, &cmd_array, &executor).await?,
            "TTL" => string::ttl(&mut socket, &cmd_array, &executor).await?,
            "KEYS" => string::keys(&mut socket, &cmd_array, &executor).await?,

            "HSET" | "HSETNX" => hash::hset(&mut socket, &cmd_array, &executor).await?,
            "HGET" => hash::hget(&mut socket, &cmd_array, &executor).await?,
            "HGETALL" => hash::hgetall(&mut socket, &cmd_array, &executor).await?,
            "HDEL" => hash::hdel(&mut socket, &cmd_array, &executor).await?,

            "LPUSH" | "RPUSH" => list::push(&mut socket, &cmd_array, &executor, &cmd_name).await?,
            "LRANGE" => list::range(&mut socket, &cmd_array, &executor).await?,
            "LPOP" | "RPOP" => list::pop(&mut socket, &cmd_array, &executor, &cmd_name).await?,
            "LLEN" => list::len(&mut socket, &cmd_array, &executor).await?,

            "SADD" => set::add(&mut socket, &cmd_array, &executor).await?,
            "SMEMBERS" => set::members(&mut socket, &cmd_array, &executor).await?,
            "SISMEMBER" => set::is_member(&mut socket, &cmd_array, &executor).await?,
            "SREM" => set::remove(&mut socket, &cmd_array, &executor).await?,

            "ZADD" => zset::add(&mut socket, &cmd_array, &executor).await?,
            "ZRANGE" => zset::range(&mut socket, &cmd_array, &executor).await?,
            "ZRANGEBYSCORE" => zset::rangebyscore(&mut socket, &cmd_array, &executor).await?,
            "ZREM" => zset::remove(&mut socket, &cmd_array, &executor).await?,

            "XADD" => stream::add(&mut socket, &cmd_array, &executor).await?,
            "XRANGE" => stream::range(&mut socket, &cmd_array, &executor).await?,
            "XLEN" => stream::len(&mut socket, &cmd_array, &executor).await?,

            "PUBLISH" => pubsub::publish(&mut socket, &cmd_array, &executor).await?,
            "SUBSCRIBE" | "PSUBSCRIBE" => {
                pubsub::subscribe(&mut socket, &cmd_array, &executor).await?
            }
            "UNSUBSCRIBE" | "PUNSUBSCRIBE" => {
                pubsub::unsubscribe(&mut socket, &cmd_array, &executor).await?
            }
            "PUBSUB" => pubsub::pubsub(&mut socket, &cmd_array, &executor).await?,

            _ => {
                RespValue::Error(format!("ERR unknown command '{}'", cmd_name))
                    .write(&mut socket)
                    .await?
            }
        }
    }
    Ok(())
}
