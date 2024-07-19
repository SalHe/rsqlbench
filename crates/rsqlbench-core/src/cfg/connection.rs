use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct User {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct Connection {
    pub host: String,
    pub port: Option<u16>,
    pub database: String,
    pub schema_user: User,
    pub tpcc_user: User,
}
