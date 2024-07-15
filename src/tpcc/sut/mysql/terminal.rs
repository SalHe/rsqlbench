use async_trait::async_trait;
use tracing::info;

use crate::tpcc::{sut::Terminal, transaction::Transaction};

pub struct MysqlTerminal {}

#[async_trait]
impl Terminal for MysqlTerminal {
    async fn execute(&self, tx: &Transaction) -> anyhow::Result<()> {
        info!(?tx);
        Ok(())
    }
}
