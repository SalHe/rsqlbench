mod generic;
mod mysql;

use async_trait::async_trait;
pub use generic::*;
pub use mysql::*;

use super::{loader::Loader, transaction::Transaction};

#[async_trait]
pub trait Sut {
    /// Make a terminal for simulate user.
    async fn terminal(&self, id: u32) -> anyhow::Result<Box<dyn Terminal>>;

    /// Build schema for TPC-C.
    async fn build_schema(&self) -> anyhow::Result<()>;

    /// After data loaded, used for building foreign keys.
    async fn after_loaded(&self) -> anyhow::Result<()>;

    /// Destroy schema created for TPC-C before.
    async fn destroy_schema(&self) -> anyhow::Result<()>;

    /// Make a loader for loading data.
    async fn loader(&self) -> anyhow::Result<Box<dyn Loader>>;
}

pub trait Terminal {
    /// Execute a transaction.
    fn execute(&self, tx: Transaction);
}

// #[derive(Debug)]
// pub struct FakeSut;

// impl Sut for FakeSut {
//     #[instrument]
//     async fn terminal(&self, id: u32) -> Box<dyn Terminal> {
//         todo!()
//     }

//     #[instrument]
//     async fn build_schema(&self) {}

//     #[instrument]
//     async fn destroy_schema(&self) {}

//     #[instrument]
//     async fn loader(&self) -> Box<dyn Loader> {
//         Box::new(FakeLoader)
//     }
// }
