use rsqlbench_core::tpcc::sut::generic_direct::Executor;
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
    async fn execute(&mut self, sql: &str) -> anyhow::Result<()> {
        let sql = sql.to_string();
        // TODO safety???(When blocking thread running but task worker abort, dangling?)
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

#[macro_export]
macro_rules! guard_yac_call {
    ($call:expr) => {
        if $call == $crate::native::EnYacResult_YAC_ERROR {
            Err($crate::wrapper::Error::get_yas_diag(None).unwrap())
        } else {
            Ok(())
        }
    };
}
