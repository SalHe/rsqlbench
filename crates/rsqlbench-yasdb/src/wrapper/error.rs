use super::diag::{get_error, DiagInfo};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0:?}")]
    YasClient(DiagInfo),
    // #[error(transparent)]
    // Other(#[from] Box<dyn std::error::Error>),
    // #[error("Unknown error")]
    // Other,
}

impl Error {
    pub fn get_yas_diag(sql: Option<String>) -> Option<Error> {
        get_error(sql).map(Error::YasClient)
    }
}
