pub trait Parser {
    fn parse(&self) {}
}

pub struct DummyParser;

impl Parser for DummyParser {}
