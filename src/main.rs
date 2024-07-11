use std::{ops::RangeInclusive, rc::Rc};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use config::{Config, Environment, File};
use rsqlbench::{
    cfg::{self, BenchConfig},
    tpcc::{
        loader::Loader,
        model::{ItemGenerator, Warehouse, WarehouseGenerator},
        sut::{MysqlSut, Sut},
    },
};
use time::{format_description::well_known::Rfc3339, UtcOffset};
use tokio::task::JoinSet;
use tracing::{info, instrument, level_filters::LevelFilter, warn};
use tracing_subscriber::{fmt::time::OffsetTime, EnvFilter};

#[instrument(skip(loader, rx))]
async fn load_warehouse(
    loader_id: usize,
    loader: Box<dyn Loader>,
    rx: async_channel::Receiver<Warehouse>,
) -> anyhow::Result<()> {
    let mut loader = loader;
    loader.load_warehouses(rx).await
}

#[instrument(skip(loader))]
async fn load_items(
    loader: Box<dyn Loader>,
    loader_id: usize,
    range: RangeInclusive<u32>,
) -> anyhow::Result<()> {
    let mut loader = loader;
    loader.load_items(ItemGenerator::new(range)).await
}

#[instrument(skip(sut))]
async fn load_all_items(sut: Rc<Box<dyn Sut>>, _: &cfg::Loader) -> anyhow::Result<()> {
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

#[derive(Debug, Clone, ValueEnum)]
enum Database {
    Mysql,
}

#[derive(Debug, Parser)]
#[command(author, version, about, long_about=None)]
struct Cli {
    /// Configuration file.
    #[arg(short, long, default_value = "rsqlbench.yaml")]
    config: String,

    /// System under test, SUT.
    #[arg(long, value_enum)]
    db: Database,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(subcommand)]
    Tpcc(TpccCommand),
}

#[derive(Debug, Subcommand)]
enum TpccCommand {
    /// Build schema and load data for TPC-C benchmark.
    Build,

    /// Benchmark TPC-C.
    Benchmark,

    /// Destroy schema.
    Destroy,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::try_parse()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .parse("")?,
        )
        .with_timer(
            OffsetTime::local_rfc_3339()
                .unwrap_or(OffsetTime::new(UtcOffset::from_hms(8, 0, 0)?, Rfc3339)),
        )
        .init();

    let cfg: BenchConfig = Config::builder()
        .add_source(File::with_name(&cli.config))
        .add_source(Environment::with_prefix("RSB"))
        .build()
        .with_context(|| "Could not load config properly.")?
        .try_deserialize()
        .with_context(|| "Could not deserialize config file.")?;

    let sut: Rc<Box<dyn Sut>> = match cli.db {
        Database::Mysql => Rc::new(Box::new(MysqlSut::new(cfg.connection))),
    };

    match cli.command {
        Command::Tpcc(tpcc_cmd) => match tpcc_cmd {
            TpccCommand::Build => {
                info!("Building schema...");
                sut.build_schema().await?;
                info!("Loading all items...");
                load_all_items(sut.clone(), &cfg.loader).await?;
                info!("Loading all warehouses...");
                load_all_warehouses(sut.clone(), &cfg.loader).await?;
                info!("Data loaded.");
                info!("Do some operations after data loading (such as building foreign keys and constraints)...");
                sut.after_loaded().await?;
            }
            TpccCommand::Benchmark => {
                warn!("not implemented!");
            }
            TpccCommand::Destroy => {
                info!("Destroying schema...");
                sut.destroy_schema().await?;
                info!("Schema Destroyed.");
            }
        },
    }

    Ok(())
}
