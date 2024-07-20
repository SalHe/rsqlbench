mod benchmark;
mod loader;

use std::rc::Rc;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use config::{Config, Environment, File};
use rsqlbench_core::{
    cfg::BenchConfig,
    tpcc::sut::{MysqlSut, Sut},
};
use time::{format_description::well_known::Rfc3339, UtcOffset};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{fmt::time::OffsetTime, EnvFilter};

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
                .from_env_lossy(),
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

    info!(?cfg, "Using config");

    let sut: Rc<Box<dyn Sut>> = match cli.db {
        Database::Mysql => Rc::new(Box::new(MysqlSut::new(cfg.connection))),
    };

    match cli.command {
        Command::Tpcc(tpcc_cmd) => match tpcc_cmd {
            TpccCommand::Build => {
                info!("Building schema...");
                sut.build_schema().await?;
                info!("Loading all items...");
                loader::load_all_items(sut.clone(), &cfg.loader).await?;
                info!("Loading all warehouses...");
                loader::load_all_warehouses(sut.clone(), &cfg.loader).await?;
                info!("Data loaded.");
                info!("Do some operations after data loading (such as building foreign keys and constraints)...");
                sut.after_loaded().await?;
            }
            TpccCommand::Benchmark => {
                info!("Prepare to benchmark...");
                benchmark::benchmark(cfg.loader.warehouse as _, sut.clone(), &cfg.benchmark.tpcc)
                    .await?;
                info!("Benchmark finished.");
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
