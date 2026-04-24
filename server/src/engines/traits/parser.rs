use crate::engines::mysql::parser::parse_to_squeal;
use crate::squeal::exec::ParseResult;
use crate::squeal::ir::Squeal;

#[allow(dead_code)]
pub trait Parser: Send + Sync {
    fn parse(&self, sql: &str) -> ParseResult<Squeal>;
}

#[allow(dead_code)]
pub struct DummyParser;

impl Parser for DummyParser {
    fn parse(&self, _sql: &str) -> ParseResult<Squeal> {
        Ok(Squeal::Sequence(vec![]))
    }
}

pub struct MysqlParser;

impl Parser for MysqlParser {
    fn parse(&self, sql: &str) -> ParseResult<Squeal> {
        parse_to_squeal(sql)
    }
}
