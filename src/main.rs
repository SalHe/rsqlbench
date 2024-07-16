use std::{
    collections::HashMap,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::Context;
use case_style::CaseStyle;
use clap::{Parser, Subcommand, ValueEnum};
use config::{Config, Environment, File};
use rsqlbench::{
    cfg::{
        self,
        tpcc::{TpccBenchmark, TpccTransaction},
        BenchConfig,
    },
    tpcc::{
        loader::Loader,
        model::{ItemGenerator, Warehouse, WarehouseGenerator, DISTRICT_PER_WAREHOUSE},
        sut::{MysqlSut, Sut, Terminal},
        transaction::Transaction,
    },
};
use time::{format_description::well_known::Rfc3339, UtcOffset};
use tokio::{
    select,
    task::{yield_now, JoinSet},
    time::{interval_at, sleep, Instant},
};
use tracing::{debug, info, instrument, level_filters::LevelFilter, trace, warn};
use tracing_subscriber::{fmt::time::OffsetTime, EnvFilter};

static TOTAL_NEW_ORDERS: AtomicU64 = AtomicU64::new(0);

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

#[instrument(skip(terminal))]
async fn tpcc_benchmark(
    terminal: Box<dyn Terminal>,
    terminal_id: usize,
    warehouse_id: u32,
    district_id: u8,
    warehouse_count: u32,
    tx_weights: TpccTransaction,
    keying: bool,
) -> anyhow::Result<()> {
    let mut terminal = terminal;
    trace!("Begin benchmarking");
    loop {
        // TODO stop
        let tx = Transaction::generate(&tx_weights, warehouse_id, district_id, warehouse_count);
        if keying {
            sleep(tx.keying_duration()).await;
        }
        debug!(?tx, "Perform transaction");
        match &tx {
            Transaction::NewOrder(input) => {
                match terminal.new_order(input).await? {
                    rsqlbench::tpcc::sut::TerminalResult::Rollbacked => {}
                    rsqlbench::tpcc::sut::TerminalResult::Executed(_) => {
                        TOTAL_NEW_ORDERS.fetch_add(1, Ordering::SeqCst);
                    }
                };
            }
            Transaction::Payment(input) => {
                terminal.payment(input).await?;
            }
            Transaction::OrderStatus(input) => {
                terminal.order_status(input).await?;
            }
            Transaction::Delivery(input) => {
                terminal.delivery(input).await?;
            }
            Transaction::StockLevel(input) => {
                terminal.stock_level(input).await?;
            }
        }
        if keying {
            sleep(tx.thinking_duration()).await;
        }
        yield_now().await;
    }
    // trace!("Benchmark finished");
    // Ok(())
}

#[instrument(skip(sut))]
async fn begin_benchmark(
    warehouses: usize,
    sut: Rc<Box<dyn Sut>>,
    tpcc: &TpccBenchmark,
) -> anyhow::Result<()> {
    let transactions = &tpcc.transactions;

    // Check weights
    let small_weight = if let Err(cfg::tpcc::Error::SmallWeight(list)) = transactions.verify() {
        list.into_iter()
            .map(|(tx, minimal)| (CaseStyle::guess(tx).unwrap().to_pascalcase(), minimal))
            .collect::<HashMap<String, f32>>()
    } else {
        HashMap::new()
    };

    let mut weight_proper = true;
    for (tx, percents) in [
        ("NewOrder", transactions.new_order_weight()),
        ("Payment", transactions.payment),
        ("OrderStatus", transactions.order_status),
        ("Delivery", transactions.stock_level),
        ("StockLevel", transactions.stock_level),
    ] {
        if let Some((_, minimal_percents)) = small_weight.get_key_value(tx) {
            weight_proper = false;
            warn!(minimal_percents, "Transaction {tx} weight = {percents:.2}%");
        } else {
            info!("Transaction {tx} weight = {percents:.2}% √");
        }
    }

    if weight_proper {
        info!("Transaction weights passed.")
    } else {
        warn!("Transaction weights got some problems.")
    }

    // Check terminals' unique warehouse/district pair
    if tpcc.terminals > (warehouses * DISTRICT_PER_WAREHOUSE) as _ {
        warn!(
            terminals = tpcc.terminals,
            warehouses, "There are too many terminals so that Clause-2.8.1.1 won't be satisfied."
        );
    }

    // Spawn terminals.
    let mut join_set = JoinSet::new();
    for terminal_id in 0..tpcc.terminals {
        let in_range_id = terminal_id % (warehouses * DISTRICT_PER_WAREHOUSE);
        let warehouse_id = (in_range_id / DISTRICT_PER_WAREHOUSE) + 1;
        let district_id = (in_range_id % DISTRICT_PER_WAREHOUSE) + 1;
        join_set.spawn(tpcc_benchmark(
            sut.terminal(terminal_id as _).await?,
            terminal_id,
            warehouse_id as u32,
            district_id as u8,
            warehouses as _,
            tpcc.transactions.clone(),
            tpcc.keying_and_thinking,
        ));
    }

    let one_minute = Duration::from_secs(60);
    let mut ticker = interval_at(Instant::now() + one_minute, one_minute);
    let mut minutes = 0;
    loop {
        select! {
            _ = ticker.tick() => {
                let total_new_orders = TOTAL_NEW_ORDERS.load(Ordering::SeqCst);
                minutes += 1;
                info!(
                    total_new_orders,
                    minutes,
                    tpmC = (total_new_orders as f64) / (minutes as f64),
                );
            }
            joined = join_set.join_next() => {
                match joined {
                    Some(j) => {
                        j??;
                    },
                    None=>{
                        break;
                    }
                }
            }
        }
    }
    while let Some(j) = join_set.join_next().await {
        j??
    }

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
                info!("Prepare to benchmark...");
                begin_benchmark(cfg.loader.warehouse as _, sut.clone(), &cfg.benchmark.tpcc)
                    .await?;
                info!("Benchmark finished...");
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
