use async_trait::async_trait;
use sqlx::{prelude::*, MySqlConnection};
use tracing::{info, instrument};

use crate::tpcc::{
    loader::{load_items, load_warehouse, Loader},
    model::{ItemGenerator, Warehouse},
};

pub struct MysqlLoader {
    conn: MySqlConnection,
}

impl MysqlLoader {
    pub fn new(conn: MySqlConnection) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl Loader for MysqlLoader {
    #[instrument(skip(self, generator))]
    async fn load_items(&mut self, generator: ItemGenerator) -> Result<(), sqlx::Error> {
        self.conn
            .transaction(|txn| Box::pin(async move { load_items(generator, txn).await }))
            .await
    }

    #[instrument(skip(self, generator))]
    async fn load_warehouses(
        &mut self,
        generator: async_channel::Receiver<Warehouse>,
    ) -> Result<(), sqlx::Error> {
        while let Ok(warehouse) = generator.recv().await {
            info!("Loading warehouse ID={id}", id = warehouse.id);
            self.conn
                .transaction(|txn| Box::pin(async move { load_warehouse(&warehouse, txn).await }))
                .await?;
        }
        Ok(())
    }
}
