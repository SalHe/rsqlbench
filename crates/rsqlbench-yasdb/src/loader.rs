use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use rsqlbench_core::tpcc::{
    self,
    loader::Loader,
    model::{ItemGenerator, Warehouse},
};
use tokio::time::sleep;
use tracing::info;

use crate::{wrapper::SimpleExecutor, Connection};

pub struct YasdbLoader {
    conn: Arc<Mutex<Connection>>,
}

impl YasdbLoader {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }
}

#[async_trait::async_trait]
impl Loader for YasdbLoader {
    async fn load_items(&mut self, generator: ItemGenerator) -> anyhow::Result<()> {
        tpcc::sut::generic_direct::load_items(
            generator,
            5000,
            &mut SimpleExecutor::new(self.conn.clone())?,
        )
        .await?;
        Ok(())
    }

    async fn load_warehouses(
        &mut self,
        generator: async_channel::Receiver<Warehouse>,
    ) -> anyhow::Result<()> {
        sleep(Duration::from_secs(1)).await;
        let mut stmt = SimpleExecutor::new(self.conn.clone())?;
        while let Ok(warehouse) = generator.recv().await {
            info!("Loading warehouse ID={id}", id = warehouse.id);
            tpcc::sut::generic_direct::load_warehouse(&warehouse, &mut stmt).await?;
        }
        Ok(())
    }
}
