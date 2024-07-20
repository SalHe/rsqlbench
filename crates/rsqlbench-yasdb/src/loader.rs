use std::time::Duration;

use rsqlbench_core::tpcc::{
    self,
    loader::Loader,
    model::{ItemGenerator, Warehouse},
};
use tokio::time::sleep;
use tracing::info;

use crate::{
    wrapper::{SimpleExecutor, StatementHandle},
    Connection,
};

pub struct YasdbLoader {
    conn: Connection,
}

impl YasdbLoader {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }
}

#[async_trait::async_trait]
impl Loader for YasdbLoader {
    async fn load_items(&mut self, generator: ItemGenerator) -> anyhow::Result<()> {
        let stmt = StatementHandle::new(&self.conn.conn_handle)?;
        tpcc::sut::all::load_items(generator, 5000, &SimpleExecutor::new(stmt)).await?;
        Ok(())
    }

    async fn load_warehouses(
        &mut self,
        generator: async_channel::Receiver<Warehouse>,
    ) -> anyhow::Result<()> {
        sleep(Duration::from_secs(1)).await;
        let stmt = SimpleExecutor::new(StatementHandle::new(&self.conn.conn_handle)?);
        while let Ok(warehouse) = generator.recv().await {
            info!("Loading warehouse ID={id}", id = warehouse.id);
            tpcc::sut::all::load_warehouse(&warehouse, &stmt).await?;
        }
        Ok(())
    }
}
