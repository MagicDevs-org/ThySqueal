pub trait Config {
    fn parse_config(&mut self, _path: String) {}
}

pub struct DummyConfig;

impl Config for DummyConfig {}
