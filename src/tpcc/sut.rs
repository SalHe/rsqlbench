mod generic;
mod mysql;

use async_trait::async_trait;
pub use generic::*;
pub use mysql::*;

use super::{
    loader::Loader,
    transaction::{Delivery, NewOrder, OrderStatus, Payment, StockLevel},
};

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

#[async_trait]
pub trait Terminal: Send {
    async fn new_order(&self, input: &NewOrder) -> anyhow::Result<TerminalResult>;
    async fn payment(&self, input: &Payment) -> anyhow::Result<()>;
    async fn order_status(&self, input: &OrderStatus) -> anyhow::Result<()>;
    async fn delivery(&self, input: &Delivery) -> anyhow::Result<()>;
    async fn stock_level(&self, input: &StockLevel) -> anyhow::Result<()>;
}

pub enum TerminalResult<T = ()> {
    Rollbacked,
    Executed(T),
}
