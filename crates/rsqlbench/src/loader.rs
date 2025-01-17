use rsqlbench_core::{
    cfg,
    tpcc::{
        loader::Loader,
        model::{ItemGenerator, Warehouse, WarehouseGenerator},
        sut::Sut,
    },
};
use std::rc::Rc;
use tokio::task::JoinSet;
use tracing::{info, instrument};

#[instrument(skip(loader, rx))]
async fn load_warehouse(
    loader_id: usize,
    loader: Box<dyn Loader>,
    rx: async_channel::Receiver<Warehouse>,
) -> anyhow::Result<()> {
    let mut loader = loader;
    loader.load_warehouses(rx).await
}

#[instrument(skip(sut))]
pub async fn load_all_items(sut: Rc<Box<dyn Sut>>, _: &cfg::Loader) -> anyhow::Result<()> {
    info!("Loading items...");
    let mut loader = sut.loader().await?;
    loader.load_items(ItemGenerator::new(1..=50000)).await?;
    loader
        .load_items(ItemGenerator::new(50001..=100000))
        .await?;
    info!("Items loaded.");
    Ok(())
}

#[instrument(skip(sut, loader_cfg))]
pub async fn load_all_warehouses(
    sut: Rc<Box<dyn Sut>>,
    loader_cfg: &cfg::Loader,
) -> anyhow::Result<()> {
    info!("Loading warehouses...");
    let mut join_set = JoinSet::new();
    let (tx_warehouse_id, rx) = async_channel::unbounded::<Warehouse>();

    for loader_id in 0..(loader_cfg.monkeys) {
        let loader = sut.loader().await?;
        join_set.spawn(load_warehouse(loader_id, loader, rx.clone()));
    }

    let warehouse = loader_cfg.warehouse;
    tokio::spawn(async move {
        let generator = WarehouseGenerator::new(1..=warehouse);
        for w in generator {
            tx_warehouse_id.send(w).await.unwrap();
        }
    })
    .await?;
    while let Some(j) = join_set.join_next().await {
        j??
    }
    info!("Warehouses loaded.");
    Ok(())
}
