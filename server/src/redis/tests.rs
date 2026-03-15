#[cfg(test)]
mod tests {
    use crate::redis::resp::{RespValue, read_value};
    use std::io::Cursor;

    #[tokio::test]
    async fn test_resp_simple_string() {
        let mut data = Cursor::new(b"+OK\r\n");
        let val = read_value(&mut data).await.unwrap();
        assert_eq!(val, RespValue::SimpleString("OK".to_string()));

        let mut buf = Vec::new();
        val.write(&mut buf).await.unwrap();
        assert_eq!(buf, b"+OK\r\n");
    }

    #[tokio::test]
    async fn test_resp_integer() {
        let mut data = Cursor::new(b":1000\r\n");
        let val = read_value(&mut data).await.unwrap();
        assert_eq!(val, RespValue::Integer(1000));

        let mut buf = Vec::new();
        val.write(&mut buf).await.unwrap();
        assert_eq!(buf, b":1000\r\n");
    }

    #[tokio::test]
    async fn test_resp_bulk_string() {
        let mut data = Cursor::new(b"$6\r\nfoobar\r\n");
        let val = read_value(&mut data).await.unwrap();
        assert_eq!(val, RespValue::BulkString(Some(b"foobar".to_vec())));

        let mut buf = Vec::new();
        val.write(&mut buf).await.unwrap();
        assert_eq!(buf, b"$6\r\nfoobar\r\n");
    }

    #[tokio::test]
    async fn test_resp_array() {
        let mut data = Cursor::new(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let val = read_value(&mut data).await.unwrap();
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"foo".to_vec())),
                RespValue::BulkString(Some(b"bar".to_vec()))
            ]))
        );

        let mut buf = Vec::new();
        val.write(&mut buf).await.unwrap();
        assert_eq!(buf, b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }
}
