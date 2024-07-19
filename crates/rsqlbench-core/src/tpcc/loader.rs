use super::model::{ItemGenerator, Warehouse};

#[async_trait::async_trait]
pub trait Loader: Send {
    async fn load_items(&mut self, generator: ItemGenerator) -> anyhow::Result<()>;
    async fn load_warehouses(
        &mut self,
        generator: async_channel::Receiver<Warehouse>,
    ) -> anyhow::Result<()>;
}
