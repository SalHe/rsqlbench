use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ConnectionsList {
    /// Connection string used when building schema.
    pub schema: String,

    /// Connection string used when loading data.
    pub loader: String,

    /// Connection string used when benchmarking.
    pub benchmark: String,

    /// Other custom configurations used for different SUT.
    #[serde(default)]
    pub others: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct Connection {
    /// SUT name.
    ///
    /// Default to database specified in connections list(if able to recognize).
    pub sut: Option<String>,

    /// Database name.
    pub database: String,

    /// Connection string list.
    pub connections: ConnectionsList,
}
