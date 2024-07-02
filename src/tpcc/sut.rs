use tracing::instrument;

use super::{
    loader::{FakeLoader, Loader},
    transaction::Transaction,
};

pub trait Sut {
    /// Make a terminal for simulate user.
    fn terminal(&self, id: u32) -> Box<dyn Terminal>;

    /// Build schema for TPC-C.
    fn build_schema(&self);

    /// Destroy schema created for TPC-C before.
    fn destroy_schema(&self);

    /// Make a loader for loading data.
    fn loader(&self) -> Box<dyn Loader>;
}

pub trait Terminal {
    /// Execute a transaction.
    fn execute(&self, tx: Transaction);
}

#[derive(Debug)]
pub struct FakeSut;

impl Sut for FakeSut {
    #[instrument]
    fn terminal(&self, id: u32) -> Box<dyn Terminal> {
        todo!()
    }

    #[instrument]
    fn build_schema(&self) {}

    #[instrument]
    fn destroy_schema(&self) {}

    #[instrument]
    fn loader(&self) -> Box<dyn Loader> {
        Box::new(FakeLoader)
    }
}
