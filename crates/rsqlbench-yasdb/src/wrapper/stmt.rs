use std::sync::{Arc, Mutex};

use rsqlbench_core::tpcc::sut::generic_direct::Executor;
use tokio::task::spawn_blocking;

use crate::{
    native::{yacDirectExecute, EnYacResult_YAC_ERROR},
    Connection,
};

use super::{Error, StatementHandle};

pub struct Statement {
    stmt: StatementHandle,
    _conn: Arc<Mutex<Connection>>, // drop after stmt
}

impl Statement {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, Error> {
        Ok(Self {
            stmt: {
                let c = conn.lock().unwrap();
                StatementHandle::new(&c.conn_handle)?
            },
            _conn: conn,
        })
    }

    pub fn handle(&self) -> &StatementHandle {
        &self.stmt
    }
}

pub struct SimpleExecutor {
    stmt: Arc<Mutex<Statement>>,
}

impl SimpleExecutor {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self, Error> {
        Ok(Self {
            stmt: Arc::new(Mutex::new(Statement::new(conn)?)),
        })
    }
}

impl Executor for SimpleExecutor {
    async fn execute(&mut self, sql: &str) -> anyhow::Result<()> {
        let sql = sql.to_string();
        let stmt = self.stmt.clone();
        spawn_blocking(move || {
            let stmt = stmt.lock().unwrap();
            if unsafe { yacDirectExecute(stmt.stmt.0, sql.as_ptr() as _, sql.len() as _) }
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
