pub const PROTO_VERSION: u8 = 10;
pub const DEFAULT_USERNAME: &str = "root";
pub const AUTH_PLUGIN_NAME: &str = "mysql_native_password";
pub const AUTH_PLUGIN_DATA_PART1: &str = "authplug";
#[allow(unused)]
pub const AUTH_PLUGIN_DATA_PART2: &str = "authplug";
pub const SERVER_VERSION: &str = "ThySqueal-0.8.0";

#[allow(unused)]
pub const CHAR_SET_UTF8: u16 = 33;
pub const CHAR_SET_CODE: u8 = 33;
pub const STATUS_FLAGS: u16 = 0x0002;

pub const CAPABILITY_LOWER: u16 = 0xF7FF;
#[allow(unused)]
pub const CAPABILITY_UPPER: u16 = 0x8000;

pub const MYSQL_NULL_TYPE: u8 = 0x06;
pub const MYSQL_TINYINT: u8 = 0x01;
pub const MYSQL_SMALLINT: u8 = 0x02;
pub const MYSQL_INT: u8 = 0x03;
pub const MYSQL_DOUBLE: u8 = 0x05;
pub const MYSQL_BIGINT: u8 = 0x08;
pub const MYSQL_DATETIME: u8 = 0x0C;
pub const MYSQL_VAR_STRING: u8 = 0xFD;
pub const MYSQL_BLOB: u8 = 0xFE;

#[allow(unused)]
pub const MYSQL_NULL_IN_BIND: u8 = 0xFB;
pub const MYSQL_OK_HEADER: u8 = 0x00;
pub const MYSQL_ERROR_HEADER: u8 = 0xFF;
pub const MYSQL_EOF_HEADER: u8 = 0xFE;

pub const LEN_ENC_2BYTE: u8 = 0xFC;
pub const LEN_ENC_3BYTE: u8 = 0xFD;
pub const LEN_ENC_8BYTE: u8 = 0xFE;

pub const COM_QUIT: u8 = 0x01;
pub const COM_INIT_DB: u8 = 0x02;
pub const COM_QUERY: u8 = 0x03;
pub const COM_FIELD_LIST: u8 = 0x04;
pub const COM_CREATE_DB: u8 = 0x05;
pub const COM_DROP_DB: u8 = 0x06;
pub const COM_STATISTICS: u8 = 0x0A;
pub const COM_PING: u8 = 0x0E;
pub const COM_STMT_PREPARE: u8 = 0x16;
pub const COM_STMT_EXECUTE: u8 = 0x17;
pub const COM_STMT_CLOSE: u8 = 0x19;
pub const COM_DEBUG: u8 = 0x0D;
pub const COM_TIME: u8 = 0x0C;
pub const COM_KILL: u8 = 0x0B;

#[allow(unused)]
pub const ERR_CODE_UNKNOWN_CMD: u16 = 1047;
#[allow(unused)]
pub const ERR_CODE_AUTH_FAILED: u16 = 1045;
#[allow(unused)]
pub const ERR_SQL_STATE: &str = "08S01";
