// use bytes::{Buf, BytesMut};
use anyhow::{Result, anyhow};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub async fn write<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<()> {
        match self {
            RespValue::SimpleString(s) => {
                writer.write_all(b"+").await?;
                writer.write_all(s.as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
            }
            RespValue::Error(s) => {
                writer.write_all(b"-").await?;
                writer.write_all(s.as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
            }
            RespValue::Integer(i) => {
                writer.write_all(b":").await?;
                writer.write_all(i.to_string().as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
            }
            RespValue::BulkString(Some(b)) => {
                writer.write_all(b"$").await?;
                writer.write_all(b.len().to_string().as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
                writer.write_all(b).await?;
                writer.write_all(b"\r\n").await?;
            }
            RespValue::BulkString(None) => {
                writer.write_all(b"$-1\r\n").await?;
            }
            RespValue::Array(Some(a)) => {
                writer.write_all(b"*").await?;
                writer.write_all(a.len().to_string().as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
                for val in a {
                    Box::pin(val.write(writer)).await?;
                }
            }
            RespValue::Array(None) => {
                writer.write_all(b"*-1\r\n").await?;
            }
        }
        Ok(())
    }
}

pub async fn read_value<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<RespValue> {
    let mut prefix = [0u8; 1];
    reader.read_exact(&mut prefix).await?;

    match prefix[0] {
        b'+' => {
            let line = read_line(reader).await?;
            Ok(RespValue::SimpleString(String::from_utf8(line)?))
        }
        b'-' => {
            let line = read_line(reader).await?;
            Ok(RespValue::Error(String::from_utf8(line)?))
        }
        b':' => {
            let line = read_line(reader).await?;
            let s = String::from_utf8(line)?;
            Ok(RespValue::Integer(s.parse()?))
        }
        b'$' => {
            let line = read_line(reader).await?;
            let len: i64 = String::from_utf8(line)?.parse()?;
            if len == -1 {
                Ok(RespValue::BulkString(None))
            } else {
                let mut data = vec![0u8; len as usize];
                reader.read_exact(&mut data).await?;
                // Skip \r\n
                let mut crlf = [0u8; 2];
                reader.read_exact(&mut crlf).await?;
                Ok(RespValue::BulkString(Some(data)))
            }
        }
        b'*' => {
            let line = read_line(reader).await?;
            let len: i64 = String::from_utf8(line)?.parse()?;
            if len == -1 {
                Ok(RespValue::Array(None))
            } else {
                let mut array = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    array.push(Box::pin(read_value(reader)).await?);
                }
                Ok(RespValue::Array(Some(array)))
            }
        }
        _ => Err(anyhow!("Invalid RESP prefix: {}", prefix[0])),
    }
}

async fn read_line<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        let mut b = [0u8; 1];
        reader.read_exact(&mut b).await?;
        if b[0] == b'\r' {
            let mut next = [0u8; 1];
            reader.read_exact(&mut next).await?;
            if next[0] == b'\n' {
                break;
            }
            line.push(b[0]);
            line.push(next[0]);
        } else {
            line.push(b[0]);
        }
    }
    Ok(line)
}
