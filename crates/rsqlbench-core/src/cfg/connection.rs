use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ConnectionsList {
    pub schema: String,
    pub loader: String,
    pub benchmark: String,
    #[serde(default)]
    pub others: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct Connection {
    /// RDBMS name.
    pub sut: Option<String>,

    /// Database name.
    pub database: String,

    /// Connection string list.
    pub connections: ConnectionsList,
}
