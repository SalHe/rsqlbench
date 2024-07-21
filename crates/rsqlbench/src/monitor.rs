use axum::{routing::get, Router};
use lazy_static::lazy_static;
use prometheus::{Gauge, IntCounter, Registry};

use crate::cfg::Monitor;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref TX_NEW_ORDER: IntCounter =
        IntCounter::new("tx_new_order", "Transaction(New Order)").expect("metric can be created");
    pub static ref TPM_NEW_ORDER: Gauge =
        Gauge::new("tpmc_new_order", "tpmC(New Order)").expect("metric can be created");
    pub static ref TX_TOTAL: IntCounter =
        IntCounter::new("tx_total", "Transaction TOTAL").expect("metric can be created");
    pub static ref TPM_TOTAL: Gauge =
        Gauge::new("tpmc_total", "tpmC TOTAL").expect("metric can be created");
}

pub fn register_registry() -> anyhow::Result<()> {
    REGISTRY.register(Box::new(TPM_NEW_ORDER.clone()))?;
    REGISTRY.register(Box::new(TX_NEW_ORDER.clone()))?;
    REGISTRY.register(Box::new(TPM_TOTAL.clone()))?;
    REGISTRY.register(Box::new(TX_TOTAL.clone()))?;
    Ok(())
}

async fn prometheus_metrics() -> String {
    let encoder: prometheus::TextEncoder = prometheus::TextEncoder::new();
    encoder
        .encode_to_string(&REGISTRY.gather())
        .expect("Failed to encode metrics for prometheus")
}

pub async fn spawn_prometheus(cfg: Monitor) -> Result<(), std::io::Error> {
    let app = Router::new().route(&cfg.path, get(prometheus_metrics));
    let listener = tokio::net::TcpListener::bind(cfg.listen_addr.clone())
        .await
        .unwrap();
    axum::serve(listener, app).await
}
