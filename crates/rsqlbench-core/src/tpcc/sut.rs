mod generic;
mod mysql;

use async_trait::async_trait;
pub use generic::*;
pub use mysql::*;

use super::{
    loader::Loader,
    transaction::{
        Delivery, DeliveryOut, NewOrder, NewOrderOut, NewOrderRollbackOut, OrderStatus,
        OrderStatusOut, Payment, PaymentOut, StockLevel, StockLevelOut,
    },
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
    async fn new_order(
        &mut self,
        input: &NewOrder,
    ) -> anyhow::Result<Result<NewOrderOut, NewOrderRollbackOut>>;
    async fn payment(&mut self, input: &Payment) -> anyhow::Result<PaymentOut>;
    async fn order_status(&mut self, input: &OrderStatus) -> anyhow::Result<OrderStatusOut>;
    async fn delivery(&mut self, input: &Delivery) -> anyhow::Result<DeliveryOut>;
    async fn stock_level(&mut self, input: &StockLevel) -> anyhow::Result<StockLevelOut>;
}

pub const TERMINAL_WIDTH: usize = 80;
pub const TERMINAL_HEIGHT: usize = 24;
pub const TERMINAL_BUFFER_LEN: usize = TERMINAL_HEIGHT * (TERMINAL_WIDTH + 1);
