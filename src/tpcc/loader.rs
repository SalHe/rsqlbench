use tracing::{debug, instrument};

use super::model::{ItemGenerator, WarehouseGenerator};

pub trait Loader {
    fn load_items(&self, generator: ItemGenerator);
    fn load_warehouses(&self, generator: WarehouseGenerator);
}

pub struct FakeLoader;

impl Loader for FakeLoader {
    #[instrument(skip(self, generator))]
    fn load_items(&self, generator: ItemGenerator) {
        for item in generator {
            debug!("Loading {item:?}");
        }
    }

    #[instrument(skip(self, generator))]
    fn load_warehouses(&self, generator: WarehouseGenerator) {
        for warehouse in generator {
            debug!("Loading {warehouse:?}");
        }
    }
}
