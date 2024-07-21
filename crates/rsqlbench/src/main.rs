mod benchmark;
mod cfg;
mod loader;
mod monitor;

use std::rc::Rc;

use anyhow::{anyhow, Context};
use cfg::RSBConfig;
use clap::{Parser, Subcommand};
use config::{Config, Environment, File};
use monitor::{register_registry, spawn_prometheus, REGISTRY};
use rsqlbench_core::{
    cfg::Connection,
    tpcc::sut::{MysqlSut, Sut},
};
#[cfg(feature = "yasdb")]
use rsqlbench_yasdb::YasdbSut;
use time::{format_description::well_known::Rfc3339, UtcOffset};
use tracing::{error, info, level_filters::LevelFilter, warn};
use tracing_subscriber::{fmt::time::OffsetTime, EnvFilter};
use url::Url;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about=None)]
struct Cli {
    /// Configuration file.
    #[arg(short, long, default_value = "rsqlbench.yaml")]
    config: String,

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

#[derive(Debug, thiserror::Error)]
enum DbVerifyError {
    #[error("Some db connection strings are wrong: {0:?}")]
    UrlError(Vec<url::ParseError>),

    #[error("Used different SUT or RDBMS in connection string list")]
    DifferentSut,
}

fn determine_db_type(connection: &Connection) -> Result<String, DbVerifyError> {
    let parsed = [
        &connection.connections.schema,
        &connection.connections.loader,
        &connection.connections.benchmark,
    ]
    .iter()
    .map(|url| Url::parse(url))
    .collect::<Vec<_>>();

    if parsed.iter().any(|x| x.is_ok()) && parsed.iter().any(|x| x.is_err()) {
        Err(DbVerifyError::UrlError(
            parsed
                .into_iter()
                .filter(|x| x.is_err())
                .map(|x| x.unwrap_err())
                .collect::<Vec<_>>(),
        ))
    } else if parsed.iter().all(Result::is_err) {
        warn!("Could not determine db type according to db connection string");

        let sut = connection.sut.clone().expect("SUT must be specified");
        warn!("Fallback SUT/db type to {}", sut);
        Ok(sut)
    } else {
        let rdbms = parsed
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .scheme()
            .to_string();
        if parsed.iter().all(|x| x.as_ref().unwrap().scheme() == rdbms) {
            Ok(rdbms.to_string())
        } else {
            Err(DbVerifyError::DifferentSut)
        }
    }
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

    let root_cfg: RSBConfig = Config::builder()
        .add_source(File::with_name(&cli.config))
        .add_source(Environment::with_prefix("RSB"))
        .build()
        .with_context(|| "Could not load config properly.")?
        .try_deserialize()
        .with_context(|| "Could not deserialize config file.")?;
    info!(?root_cfg, "Using config");

    let cfg = root_cfg.bench;
    let sut_type = determine_db_type(&cfg.connection)?;

    info!(sut_type);
    let sut: Rc<Box<dyn Sut>> = match sut_type.as_str() {
        "mysql" => Rc::new(Box::new(MysqlSut::new(cfg.connection))),
        #[cfg(feature = "yasdb")]
        "yasdb" => Rc::new(Box::new(YasdbSut::new(cfg.connection))),
        #[cfg(not(feature = "yasdb"))]
        "yasdb" => return Err(anyhow!("yasdb not implement in current rsqlbench distribution, please compile rsqlbench with feature `yasdb`.")),
        _ => return Err(anyhow!("Unsupported sut/db.")),
    };

    register_registry()?;

    let prometheus = if let Some(monitor) = root_cfg.monitor {
        if monitor.enable {
            info!("Enable REST API for prometheus");
            Some(tokio::spawn(spawn_prometheus(monitor)))
        } else {
            None
        }
    } else {
        None
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
                let encoder = prometheus::TextEncoder::new();
                let gathered = encoder.encode_to_string(&REGISTRY.gather())?;
                info!("Benchmark finished.");
                info!("{gathered}");
            }
            TpccCommand::Destroy => {
                info!("Destroying schema...");
                sut.destroy_schema().await?;
                info!("Schema Destroyed.");
            }
        },
    }

    if let Some(p) = prometheus {
        p.abort();
    }

    Ok(())
}
