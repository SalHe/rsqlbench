use super::model::{ItemGenerator, Warehouse};

#[async_trait::async_trait]
pub trait Loader: Send {
    async fn load_items(&mut self, generator: ItemGenerator) -> Result<(), sqlx::Error>;
    async fn load_warehouses(
        &mut self,
        generator: async_channel::Receiver<Warehouse>,
    ) -> Result<(), sqlx::Error>;
}

// pub struct FakeLoader;

// #[async_trait::async_trait]
// impl Loader for FakeLoader {
//     #[instrument(skip(self, generator))]
//     async fn load_items(&self, generator: ItemGenerator) {
//         for item in generator {
//             debug!("Loading {item:?}");
//         }
//     }

//     #[instrument(skip(self, generator))]
//     async fn load_warehouses(&self, generator: &mut async_channel::Receiver<Warehouse>) {
//         while let Ok(warehouse) = generator.recv().await {
//             debug!("Loading {warehouse:?}");
//         }
//     }
// }
