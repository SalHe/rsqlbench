use async_trait::async_trait;
use sqlx::{prelude::*, MySqlConnection};
use tracing::{info, instrument};

use crate::tpcc::{
    loader::Loader,
    model::{ItemGenerator, Warehouse},
    sut::generic_direct::{self, SqlxExecutorWrapper},
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
    async fn load_items(&mut self, generator: ItemGenerator) -> anyhow::Result<()> {
        self.conn
            .transaction(|txn| {
                Box::pin(async move {
                    generic_direct::load_items(
                        generator,
                        50000,
                        &mut SqlxExecutorWrapper::new(&mut **txn),
                    )
                    .await
                })
            })
            .await?;
        Ok(())
    }

    #[instrument(skip(self, generator))]
    async fn load_warehouses(
        &mut self,
        generator: async_channel::Receiver<Warehouse>,
    ) -> anyhow::Result<()> {
        sqlx::query("set autocommit = 1")
            .execute(&mut self.conn)
            .await?;
        while let Ok(warehouse) = generator.recv().await {
            info!("Loading warehouse ID={id}", id = warehouse.id);
            self.conn
                .transaction(|txn| {
                    Box::pin(async move {
                        generic_direct::load_warehouse(
                            &warehouse,
                            &mut SqlxExecutorWrapper::new(&mut **txn),
                        )
                        .await
                    })
                })
                .await?;
        }
        Ok(())
    }
}
