use rsqlbench_core::cfg::BenchConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Monitor {
    #[serde(default = "default_monitor_enable")]
    pub enable: bool,
    pub listen_addr: String,
    #[serde(default = "default_monitor_api_path")]
    pub path: String,
}

fn default_monitor_api_path() -> String {
    "/prometheus".to_string()
}

fn default_monitor_enable() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct RSBConfig {
    pub monitor: Option<Monitor>,
    pub bench: BenchConfig,
}
