use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Loader {
    /// Parallelism for loading data.
    pub monkeys: usize,

    /// Count of warehouses.
    pub warehouse: u32,
}
