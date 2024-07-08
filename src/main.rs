use std::{cmp::min, ops::RangeInclusive, rc::Rc};

use anyhow::Context;
use rsqlbench::{
    cfg::{self, BenchConfig},
    tpcc::{
        loader::Loader,
        model::{ItemGenerator, Warehouse, WarehouseGenerator, MAX_ITEMS},
        sut::{MysqlSut, Sut},
    },
};
use tokio::task::JoinSet;
use tracing::{info, instrument, level_filters::LevelFilter};
use tracing_subscriber::{fmt::time::OffsetTime, EnvFilter};

#[instrument(skip(loader, rx))]
async fn load_warehouse(
    loader_id: usize,
    loader: Box<dyn Loader>,
    rx: async_channel::Receiver<Warehouse>,
) -> Result<(), sqlx::Error> {
    let mut loader = loader;
    loader.load_warehouses(rx).await
}

#[instrument(skip(loader))]
async fn load_items(
    loader: Box<dyn Loader>,
    loader_id: usize,
    range: RangeInclusive<u32>,
) -> Result<(), sqlx::Error> {
    let mut loader = loader;
    loader.load_items(ItemGenerator::new(range)).await
}

#[instrument(skip(sut, loader_cfg))]
async fn load_all_items(sut: Rc<Box<dyn Sut>>, loader_cfg: &cfg::Loader) -> anyhow::Result<()> {
    info!("Loading items...");
    let mut join_set = JoinSet::new();
    static ITEMS_COUNT: u32 = MAX_ITEMS as _;
    let batch = min(ITEMS_COUNT / (loader_cfg.monkeys as u32), 100);
    for i in 0..loader_cfg.monkeys {
        let offset = batch * (i as u32);
        let rng = if i < loader_cfg.monkeys - 1 {
            (offset + 1)..=(offset + batch)
        } else {
            (offset + 1)..=ITEMS_COUNT
        };
        join_set.spawn(load_items(sut.loader().await?, i, rng));
    }

    while let Some(j) = join_set.join_next().await {
        j??
    }
    info!("Items loaded.");
    Ok(())
}

#[instrument(skip(sut, loader_cfg))]
async fn load_all_warehouses(
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .parse("")?,
        )
        .with_timer(OffsetTime::local_rfc_3339()?)
        .init();

    info!("Hello tpcc");

    let cfg = BenchConfig::new(Some("rsqlbench-dev"))
        .with_context(|| "Could not load config properly.")?;

    let sut: Rc<Box<dyn Sut>> = Rc::new(Box::new(MysqlSut::new(cfg.connection)));

    sut.build_schema().await?;

    load_all_items(sut.clone(), &cfg.loader).await?;
    load_all_warehouses(sut.clone(), &cfg.loader).await?;

    sut.after_loaded().await?;
    // sut.destroy_schema().await?;

    Ok(())
}
