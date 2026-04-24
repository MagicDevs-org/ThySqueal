use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::engines::mysql::error::SqlError;
use crate::squeal::exec::QueryResult;
use crate::storage::Value;

use super::constants::*;

pub async fn read_packet(socket: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await?;

    let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let seq = header[3];

    let mut payload = vec![0u8; len];
    socket.read_exact(&mut payload).await?;

    Ok((seq, payload))
}

pub async fn send_packet(socket: &mut TcpStream, seq: u8, payload: &[u8]) -> Result<()> {
    let len = payload.len();
    let mut header = [0u8; 4];
    header[0] = (len & 0xFF) as u8;
    header[1] = ((len >> 8) & 0xFF) as u8;
    header[2] = ((len >> 16) & 0xFF) as u8;
    header[3] = seq;

    socket.write_all(&header).await?;
    socket.write_all(payload).await?;
    Ok(())
}

pub fn write_len_enc_int(buf: &mut Vec<u8>, val: u64) {
    if val < 251 {
        buf.push(val as u8);
    } else if val < 0x10000 {
        buf.push(0xFC);
        WriteBytesExt::write_u16::<LittleEndian>(buf, val as u16).unwrap();
    } else if val < 0x1000000 {
        buf.push(0xFD);
        let bytes = (val as u32).to_le_bytes();
        buf.extend_from_slice(&bytes[..3]);
    } else {
        buf.push(0xFE);
        WriteBytesExt::write_u64::<LittleEndian>(buf, val).unwrap();
    }
}

pub fn write_len_enc_str(buf: &mut Vec<u8>, s: &str) {
    write_len_enc_int(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

pub async fn send_handshake(socket: &mut TcpStream) -> Result<String> {
    let mut payload = Vec::new();

    payload.push(PROTO_VERSION);
    write_len_enc_str(&mut payload, SERVER_VERSION);

    write_len_enc_int(&mut payload, 1u64);
    payload.push(CHAR_SET_CODE);

    payload.extend_from_slice(&STATUS_FLAGS.to_le_bytes());
    payload.extend_from_slice(&CAPABILITY_LOWER.to_le_bytes());
    payload.push(CHAR_SET_CODE);

    payload.extend_from_slice(&[0u8; 10]);

    let challenge = generate_challenge();
    write_len_enc_str(&mut payload, &challenge);
    payload.push(0);

    write_len_enc_str(&mut payload, AUTH_PLUGIN_NAME);
    payload.push(0);

    send_packet(socket, 0, &payload).await?;
    Ok(challenge)
}

fn generate_challenge() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..20).map(|_| rng.r#gen::<u8>() as char).collect()
}

pub async fn send_ok_packet(socket: &mut TcpStream, seq: u8, msg: &str) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(MYSQL_OK_HEADER);
    write_len_enc_int(&mut payload, 0);
    payload.extend_from_slice(&STATUS_FLAGS.to_le_bytes());
    write_len_enc_str(&mut payload, msg);
    send_packet(socket, seq, &payload).await
}

pub async fn send_sql_error(socket: &mut TcpStream, seq: u8, err: &SqlError) -> Result<()> {
    let payload = err.to_string();
    let code = err.mysql_errno();
    let state = err.mysql_sqlstate();
    let mut pkt = Vec::new();
    pkt.push(MYSQL_ERROR_HEADER);
    WriteBytesExt::write_u16::<LittleEndian>(&mut pkt, code)?;
    pkt.push(b'#');
    pkt.extend_from_slice(state.as_bytes());
    pkt.extend_from_slice(payload.as_bytes());
    send_packet(socket, seq, &pkt).await
}

pub async fn send_resultset(
    socket: &mut TcpStream,
    mut seq: u8,
    result: QueryResult,
) -> Result<()> {
    if result.columns.is_empty() {
        let mut pkt = Vec::new();
        write_len_enc_int(&mut pkt, result.rows_affected);
        pkt.push(0);
        pkt.extend_from_slice(&STATUS_FLAGS.to_le_bytes());
        send_packet(socket, seq, &pkt).await?;
        return Ok(());
    }

    for col in &result.columns {
        let mut col_pkt = Vec::new();
        write_len_enc_str(&mut col_pkt, "");
        write_len_enc_str(&mut col_pkt, "");
        write_len_enc_str(&mut col_pkt, "");
        write_len_enc_str(&mut col_pkt, col);
        col_pkt.push(0);
        col_pkt.push(MYSQL_VAR_STRING);
        col_pkt.push(0);
        col_pkt.extend_from_slice(&((255u16).to_le_bytes()));
        send_packet(socket, seq, &col_pkt).await?;
        seq += 1;
    }

    let mut eof_pkt = vec![MYSQL_EOF_HEADER];
    write_len_enc_int(&mut eof_pkt, 0);
    eof_pkt.extend_from_slice(&STATUS_FLAGS.to_le_bytes());
    send_packet(socket, seq, &eof_pkt).await?;
    seq += 1;

    for row in &result.rows {
        let mut row_pkt = Vec::new();
        for val in row {
            match val {
                Value::Null => row_pkt.push(0xFB),
                _ => {
                    let bytes = val.to_mysql_bytes();
                    write_len_enc_int(&mut row_pkt, bytes.len() as u64);
                    row_pkt.extend_from_slice(&bytes);
                }
            }
        }
        send_packet(socket, seq, &row_pkt).await?;
        seq += 1;
    }

    let mut eof_pkt = vec![MYSQL_EOF_HEADER];
    write_len_enc_int(&mut eof_pkt, 0);
    eof_pkt.extend_from_slice(&0u16.to_le_bytes());
    send_packet(socket, seq, &eof_pkt).await?;

    Ok(())
}

pub async fn send_stmt_prepare_ok(
    socket: &mut TcpStream,
    seq: u8,
    stmt_id: u64,
    num_columns: u16,
    num_params: u16,
) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(0);
    write_len_enc_int(&mut payload, stmt_id);
    write_len_enc_int(&mut payload, num_columns as u64);
    write_len_enc_int(&mut payload, num_params as u64);
    payload.push(0);
    payload.extend_from_slice(&[0u8; 1]);
    send_packet(socket, seq, &payload).await
}
