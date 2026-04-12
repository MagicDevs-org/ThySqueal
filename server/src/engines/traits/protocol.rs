use crate::squeal::exec::Executor;
use anyhow::Result;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;

#[allow(dead_code)]
pub trait Protocol: Send + Sync {
    fn name(&self) -> &'static str;

    fn port_key(&self) -> &'static str {
        self.name()
    }

    fn create(executor: Arc<Executor>) -> Self
    where
        Self: Sized;

    fn run<'a>(&'a self, addr: &'a str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

#[allow(dead_code)]
pub struct DummyProtocol;

impl Protocol for DummyProtocol {
    fn name(&self) -> &'static str {
        "dummy"
    }

    fn create(_executor: Arc<Executor>) -> Self
    where
        Self: Sized,
    {
        Self
    }

    fn run<'a>(&'a self, _addr: &'a str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}
