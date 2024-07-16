use std::time::Duration;

use async_trait::async_trait;
use tokio::time::sleep;
use tracing::debug;

use crate::tpcc::{
    sut::{Terminal, TerminalResult},
    transaction::{Delivery, NewOrder, OrderStatus, Payment, StockLevel},
};

pub struct MysqlTerminal {}

#[async_trait]
impl Terminal for MysqlTerminal {
    async fn new_order(&self, _input: &NewOrder) -> anyhow::Result<TerminalResult> {
        debug!("not implemented!");
        sleep(Duration::from_micros(50)).await;
        Ok(TerminalResult::Executed(()))
    }

    async fn payment(&self, _input: &Payment) -> anyhow::Result<()> {
        debug!("not implemented!");
        sleep(Duration::from_micros(50)).await;
        Ok(())
    }

    async fn order_status(&self, _input: &OrderStatus) -> anyhow::Result<()> {
        debug!("not implemented!");
        sleep(Duration::from_micros(50)).await;
        Ok(())
    }

    async fn delivery(&self, _input: &Delivery) -> anyhow::Result<()> {
        debug!("not implemented!");
        sleep(Duration::from_micros(50)).await;
        Ok(())
    }

    async fn stock_level(&self, _input: &StockLevel) -> anyhow::Result<()> {
        debug!("not implemented!");
        sleep(Duration::from_micros(50)).await;
        Ok(())
    }
}
