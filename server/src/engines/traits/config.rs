#[allow(dead_code)]
pub trait Config {
    fn parse_config(&mut self, _path: String) {}
}

#[allow(dead_code)]
pub struct DummyConfig;

impl Config for DummyConfig {}
