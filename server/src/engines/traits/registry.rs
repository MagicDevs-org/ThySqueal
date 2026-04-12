use crate::engines::traits::Engine;

pub struct Registry {
    pub engines: Vec<Box<dyn Engine>>,
}
