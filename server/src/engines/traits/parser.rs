#[allow(dead_code)]
pub trait Parser {
    fn parse(&self) {}
}

#[allow(dead_code)]
pub struct DummyParser;

impl Parser for DummyParser {}
