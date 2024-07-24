use std::sync::{Arc, Mutex};

use rsqlbench_core::{
    cfg::Connection as ConnectionCfg,
    tpcc::{
        loader::Loader,
        sut::{generic_direct::Executor, Sut, Terminal},
    },
};
use tokio::task::spawn_blocking;
use tracing::{info, warn};

use crate::{
    native::{yacConnect, EnYacResult_YAC_ERROR},
    wrapper::{DbcHandle, EnvHandle, Error, SimpleExecutor},
    Connection, YasdbLoader, YasdbTerminal,
};

pub struct YasdbSut {
    connection: ConnectionCfg,
    warehouse_count: u32,
}

impl YasdbSut {
    pub fn new(connection: ConnectionCfg, warehouse_count: u32) -> Self {
        Self {
            connection,
            warehouse_count,
        }
    }

    async fn connect(&self, url: String) -> anyhow::Result<Connection> {
        let username = self.connection.connections.others["username"].clone();
        let password = self.connection.connections.others["password"].clone();
        let conn = spawn_blocking(move || {
            let env_handle = EnvHandle::new()?.with_utf8();
            let conn_handle = DbcHandle::new(&env_handle)?;
            let result = unsafe {
                yacConnect(
                    conn_handle.0,
                    url.as_ptr() as _,
                    url.len() as _,
                    username.as_ptr() as _,
                    username.len() as _,
                    password.as_ptr() as _,
                    password.len() as _,
                )
            };

            if result == EnYacResult_YAC_ERROR {
                return Err(Error::get_yas_diag(None).unwrap());
            }
            Ok(Connection {
                conn_handle,
                _env_handle: env_handle,
            })
        })
        .await?;
        Ok(conn?)
    }
}

#[async_trait::async_trait]
impl Sut for YasdbSut {
    async fn terminal(&self, _id: u32) -> anyhow::Result<Box<dyn Terminal>> {
        Ok(Box::new(YasdbTerminal::new(
            self.connect(self.connection.connections.benchmark.clone())
                .await?,
            self.warehouse_count,
        )))
    }

    async fn build_schema(&self) -> anyhow::Result<()> {
        let conn = self
            .connect(self.connection.connections.schema.to_string())
            .await?;
        // let db_ddl = format!("create database {}", self.connection.database);
        let sql_set = [
            // `drop database` is not supported, so database should be created manually.
            // db_ddl.as_str(),
            r"CREATE TABLE CUSTOMER (C_ID NUMBER(5, 0), C_D_ID NUMBER(2, 0), C_W_ID NUMBER(6, 0), C_FIRST VARCHAR2(16), C_MIDDLE CHAR(2), C_LAST VARCHAR2(16), C_STREET_1 VARCHAR2(20), C_STREET_2 VARCHAR2(20), C_CITY VARCHAR2(20), C_STATE CHAR(2), C_ZIP CHAR(9), C_PHONE CHAR(16), C_SINCE DATE, C_CREDIT CHAR(2), C_CREDIT_LIM NUMBER(12, 2), C_DISCOUNT NUMBER(4, 4), C_BALANCE NUMBER(12, 2), C_YTD_PAYMENT NUMBER(12, 2), C_PAYMENT_CNT NUMBER(8, 0), C_DELIVERY_CNT NUMBER(8, 0), C_DATA VARCHAR2(500))",
            r"CREATE TABLE DISTRICT (D_ID NUMBER(2, 0), D_W_ID NUMBER(6, 0), D_YTD NUMBER(12, 2), D_TAX NUMBER(4, 4), D_NEXT_O_ID NUMBER, D_NAME VARCHAR2(10), D_STREET_1 VARCHAR2(20), D_STREET_2 VARCHAR2(20), D_CITY VARCHAR2(20), D_STATE CHAR(2), D_ZIP CHAR(9))",
            r"CREATE TABLE HISTORY (H_C_ID NUMBER, H_C_D_ID NUMBER, H_C_W_ID NUMBER, H_D_ID NUMBER, H_W_ID NUMBER, H_DATE DATE, H_AMOUNT NUMBER(6, 2), H_DATA VARCHAR2(24)) ",
            r"CREATE TABLE ITEM (I_ID NUMBER(6, 0), I_IM_ID NUMBER, I_NAME VARCHAR2(24), I_PRICE NUMBER(5, 2), I_DATA VARCHAR2(50))",
            r"CREATE TABLE WAREHOUSE (W_ID NUMBER(6, 0), W_YTD NUMBER(12, 2), W_TAX NUMBER(4, 4), W_NAME VARCHAR2(10), W_STREET_1 VARCHAR2(20), W_STREET_2 VARCHAR2(20), W_CITY VARCHAR2(20), W_STATE CHAR(2), W_ZIP CHAR(9))",
            r"CREATE TABLE STOCK (S_I_ID NUMBER(6, 0), S_W_ID NUMBER(6, 0), S_QUANTITY NUMBER(6, 0), S_DIST_01 CHAR(24), S_DIST_02 CHAR(24), S_DIST_03 CHAR(24), S_DIST_04 CHAR(24), S_DIST_05 CHAR(24), S_DIST_06 CHAR(24), S_DIST_07 CHAR(24), S_DIST_08 CHAR(24), S_DIST_09 CHAR(24), S_DIST_10 CHAR(24), S_YTD NUMBER(10, 0), S_ORDER_CNT NUMBER(6, 0), S_REMOTE_CNT NUMBER(6, 0), S_DATA VARCHAR2(50))",
            r"CREATE TABLE NEW_ORDER (NO_W_ID NUMBER, NO_D_ID NUMBER, NO_O_ID NUMBER, CONSTRAINT INORD PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID) ENABLE)",
            r"CREATE TABLE OORDER (O_ID NUMBER, O_W_ID NUMBER, O_D_ID NUMBER, O_C_ID NUMBER, O_CARRIER_ID NUMBER, O_OL_CNT NUMBER, O_ALL_LOCAL NUMBER, O_ENTRY_D DATE)",
            r"CREATE TABLE ORDER_LINE (OL_W_ID NUMBER, OL_D_ID NUMBER, OL_O_ID NUMBER, OL_NUMBER NUMBER, OL_I_ID NUMBER, OL_DELIVERY_D DATE, OL_AMOUNT NUMBER, OL_SUPPLY_W_ID NUMBER, OL_QUANTITY NUMBER, OL_DIST_INFO CHAR(24), CONSTRAINT IORDL PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER) ENABLE)",
        ];
        let mut exec = SimpleExecutor::new(Arc::new(Mutex::new(conn)))?;
        for sql in sql_set {
            info!(ddl = sql, "Creating table");
            exec.execute(sql).await?;
        }
        Ok(())
    }

    async fn after_loaded(&self) -> anyhow::Result<()> {
        let conn = self
            .connect(self.connection.connections.schema.to_string())
            .await?;
        let mut exec = SimpleExecutor::new(Arc::new(Mutex::new(conn)))?;

        // Build indexes
        let sql_set = [
            "CREATE UNIQUE INDEX CUSTOMER_I1 ON CUSTOMER (C_W_ID, C_D_ID, C_ID)",
            "CREATE UNIQUE INDEX CUSTOMER_I2 ON CUSTOMER (C_LAST, C_D_ID, C_W_ID, C_FIRST)",
            "CREATE UNIQUE INDEX DISTRICT_I1 ON DISTRICT (D_W_ID, D_ID)",
            "CREATE UNIQUE INDEX ITEM_I1 ON ITEM (I_ID)",
            "CREATE UNIQUE INDEX OORDER_I1 ON OORDER (O_W_ID, O_D_ID, O_ID)",
            "CREATE UNIQUE INDEX OORDER_I2 ON OORDER (O_W_ID, O_D_ID, O_C_ID, O_ID)",
            "CREATE UNIQUE INDEX STOCK_I1 ON STOCK (S_I_ID, S_W_ID)",
            "CREATE UNIQUE INDEX WAREHOUSE_I1 ON WAREHOUSE (W_ID)",
        ];
        info!("Building indexes...");
        for sql in sql_set {
            exec.execute(sql).await?;
        }
        info!("Indexes created.");

        info!("Creating view stock_item...");
        exec.execute("CREATE OR REPLACE VIEW STOCK_ITEM (I_ID, S_W_ID, I_PRICE, I_NAME, I_DATA, S_DATA, S_QUANTITY, S_ORDER_CNT, S_YTD, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10) AS SELECT /*+ LEADING(S) USE_NL(I) */ I.I_ID, S_W_ID, I.I_PRICE, I.I_NAME, I.I_DATA, S_DATA, S_QUANTITY, S_ORDER_CNT, S_YTD, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 FROM STOCK S, ITEM I WHERE I.I_ID = S.S_I_ID").await?;
        info!("Views created.");

        // Create procedures
        let sql_set = [
            include_str!("../sql/new_order.sql"),
            include_str!("../sql/payment.sql"),
            include_str!("../sql/delivery.sql"),
            include_str!("../sql/order_status.sql"),
            include_str!("../sql/stock_level.sql"),
        ];
        info!("Creating procedures...");
        for sql in sql_set {
            exec.execute(sql).await?;
        }
        info!("Procedures created.");

        Ok(())
    }

    async fn destroy_schema(&self) -> anyhow::Result<()> {
        let conn = self
            .connect(self.connection.connections.schema.to_string())
            .await?;
        let conn = Arc::new(Mutex::new(conn));

        // Drop tables
        let tables = [
            "CUSTOMER",
            "DISTRICT",
            "HISTORY",
            "ITEM",
            "WAREHOUSE",
            "STOCK",
            "NEW_ORDER",
            "OORDER",
            "ORDER_LINE",
        ];
        let mut exec = SimpleExecutor::new(conn.clone())?;
        for table in tables {
            info!(table, "Dropping table");
            match exec.execute(&format!("drop table {table}")).await {
                Ok(_) => info!(table, "Table dropped"),
                Err(e) => warn!(table, ?e, "Failed to drop table"),
            }
        }

        // Drop procedures
        let procedures = ["NEWORD", "OSTAT", "PAYMENT", "DELIVERY", "SLEV"];
        let mut exec = SimpleExecutor::new(conn)?;
        for procedure in procedures {
            info!(procedure, "Dropping procedure");
            match exec.execute(&format!("drop procedure {procedure}")).await {
                Ok(_) => info!(procedure, "Table dropped"),
                Err(e) => warn!(procedure, ?e, "Failed to drop procedure"),
            }
        }
        Ok(())
    }

    async fn loader(&self) -> anyhow::Result<Box<dyn Loader>> {
        Ok(Box::new(YasdbLoader::new(
            self.connect(self.connection.connections.loader.clone())
                .await?,
        )))
    }
}
