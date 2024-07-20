use rsqlbench_core::tpcc::sut::all::Executor;
use tokio::task::spawn_blocking;

use crate::native::{yacDirectExecute, EnYacResult_YAC_ERROR};

use super::{Error, StatementHandle};

pub struct SimpleExecutor {
    stmt: StatementHandle,
}

impl SimpleExecutor {
    pub fn new(stmt: StatementHandle) -> Self {
        Self { stmt }
    }
}

impl Executor for SimpleExecutor {
    async fn execute(&self, sql: &str) -> anyhow::Result<()> {
        let sql = sql.to_string();
        // TODO safety???
        let s = unsafe { &*(self as *const Self) };
        spawn_blocking(move || {
            if unsafe { yacDirectExecute(s.stmt.0, sql.as_ptr() as _, sql.len() as _) }
                == EnYacResult_YAC_ERROR
            {
                Err(Error::get_yas_diag(Some(sql.to_string())).unwrap().into())
            } else {
                Ok(())
            }
        })
        .await?
    }
}
