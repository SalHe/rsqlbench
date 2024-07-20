use std::{
    collections::HashMap,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::anyhow;
use case_style::CaseStyle;
use rsqlbench_core::{
    cfg::{
        self,
        tpcc::{TpccBenchmark, TpccTransaction},
    },
    tpcc::{
        model::DISTRICT_PER_WAREHOUSE,
        sut::{Sut, Terminal},
        transaction::Transaction,
    },
};
use tokio::{
    select,
    sync::broadcast,
    task::{yield_now, JoinSet},
    time::{interval_at, sleep, Instant},
};

use tracing::{debug, error, info, instrument, trace, warn};

static TOTAL_NEW_ORDERS: AtomicU64 = AtomicU64::new(0);
static TOTAL_TRANSACTIONS: AtomicU64 = AtomicU64::new(0);

#[allow(clippy::too_many_arguments)] // TODO
#[instrument(skip(terminal, rx_stop))]
async fn tpcc_benchmark(
    terminal: Box<dyn Terminal>,
    terminal_id: usize,
    warehouse_id: u32,
    district_id: u8,
    warehouse_count: u32,
    tx_weights: TpccTransaction,
    keying: bool,
    rx_stop: broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    let mut rx_stop = rx_stop;
    let mut terminal = terminal;
    trace!("Begin benchmarking");
    loop {
        let tx = Transaction::generate(&tx_weights, warehouse_id, district_id, warehouse_count);
        if rx_stop.try_recv().is_ok() {
            break;
        }
        if keying {
            sleep(tx.keying_duration()).await;
        }
        debug!(?tx, "Perform transaction");
        match &tx {
            Transaction::NewOrder(input) => {
                trace!(%input);
                match terminal.new_order(input).await? {
                    Ok(out) => trace!(%out, "New order created"),
                    Err(rb) => trace!(%rb, "Failed to create new order"),
                }
                if !input.rollback_last {
                    TOTAL_NEW_ORDERS.fetch_add(1, Ordering::SeqCst);
                }
            }
            Transaction::Payment(input) => {
                trace!(%input);
                let out = terminal.payment(input).await?;
                trace!(%out, "Paid");
            }
            Transaction::OrderStatus(input) => {
                trace!(%input);
                let out = terminal.order_status(input).await?;
                trace!(%out, "Query order status");
            }
            Transaction::Delivery(input) => {
                trace!(%input);
                let out = terminal.delivery(input).await?;
                trace!(%out, "Delivery orders");
            }
            Transaction::StockLevel(input) => {
                trace!(%input);
                let out = terminal.stock_level(input).await?;
                trace!(%out, "Query stock level");
            }
        }
        TOTAL_TRANSACTIONS.fetch_add(1, Ordering::SeqCst);
        if keying {
            sleep(tx.thinking_duration()).await;
        }
        yield_now().await;
    }
    trace!("Benchmark finished");
    Ok(())
}

fn check_weight(tpcc: &TpccBenchmark, warehouses: usize) -> anyhow::Result<()> {
    let transactions = &tpcc.transactions;
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
            if percents < 0.0 {
                error!("You must be kidding: Transaction {tx} weight = {percents:.2}%");
                return Err(anyhow!("Negative weight {percents:.2}% for {tx}"));
            }
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
    Ok(())
}

async fn spawn_terminals(
    warehouses: usize,
    sut: Rc<Box<dyn Sut>>,
    tpcc: &TpccBenchmark,
    tx_stop: &broadcast::Sender<()>,
) -> anyhow::Result<JoinSet<Result<(), anyhow::Error>>> {
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
            tx_stop.subscribe(),
        ));
    }
    Ok(join_set)
}

async fn wait_for_benchmark(
    tpcc: &TpccBenchmark,
    join_set: JoinSet<Result<(), anyhow::Error>>,
    tx_stop: broadcast::Sender<()>,
) -> anyhow::Result<()> {
    let mut join_set = join_set;
    let one_minute = Duration::from_secs(60);
    let mut ramp_up: Option<(u64, u64)> = if tpcc.ramp_up == 0 {
        Some((0, 0))
    } else {
        None
    };
    let mut ticker = interval_at(Instant::now() + one_minute, one_minute);
    let mut minutes = 0;
    loop {
        select! {
            _ = ticker.tick() => {
                let mut total_new_orders = TOTAL_NEW_ORDERS.load(Ordering::SeqCst);
                let mut total_transactions = TOTAL_TRANSACTIONS.load(Ordering::SeqCst);
                if let Some((no, tx)) = ramp_up {
                    total_new_orders -= no;
                    total_transactions -= tx;
                }
                minutes += 1;
                info!(
                    minutes,
                    total_new_orders,
                    total_transactions,
                    baking = ramp_up.is_some(),
                    tpmC_NewOrder = (total_new_orders as f64) / (minutes as f64),
                    tpmTOTAL = (total_transactions as f64) / (minutes as f64),
                );
                if minutes == tpcc.ramp_up && ramp_up.is_none() {
                    info!("Ramp up finished");
                    ramp_up = Some((total_new_orders, total_transactions));
                    minutes = 0;
                } else if minutes == tpcc.baking && ramp_up.is_some() {
                    tx_stop.send(()).unwrap();
                    break;
                }
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

    let (total_new_orders, total_transactions) = ramp_up.unwrap();
    info!(
        total_new_orders,
        total_transactions,
        tpmC_NewOrder = (total_new_orders as f64) / (tpcc.ramp_up as f64),
        tpmTOTAL = (total_transactions as f64) / (tpcc.ramp_up as f64),
        "Result during Ramp up"
    );
    let total_new_orders = TOTAL_NEW_ORDERS.load(Ordering::SeqCst) - total_new_orders;
    let total_transactions = TOTAL_TRANSACTIONS.load(Ordering::SeqCst) - total_transactions;
    info!(
        total_new_orders,
        total_transactions,
        tpmC_NewOrder = (total_new_orders as f64) / (tpcc.baking as f64),
        tpmTOTAL = (total_transactions as f64) / (tpcc.baking as f64),
        "Result for Benchmark"
    );
    while let Some(j) = join_set.join_next().await {
        j??
    }
    Ok(())
}

#[instrument(skip(sut, tpcc))]
pub async fn benchmark(
    warehouses: usize,
    sut: Rc<Box<dyn Sut>>,
    tpcc: &TpccBenchmark,
) -> anyhow::Result<()> {
    check_weight(tpcc, warehouses)?;
    let (tx_stop, _) = broadcast::channel::<()>(1);
    let join_set = spawn_terminals(warehouses, sut, tpcc, &tx_stop).await?;
    wait_for_benchmark(tpcc, join_set, tx_stop).await?;
    check_weight(tpcc, warehouses)?; // report weights again
    Ok(())
}
