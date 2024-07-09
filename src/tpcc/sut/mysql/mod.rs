mod loader;

use crate::cfg::Connection;
use crate::tpcc::loader::Loader;
use async_trait::async_trait;
use loader::MysqlLoader;
use sqlx::mysql::MySqlConnectOptions;

use sqlx::{ConnectOptions, Executor};
use tracing::instrument;

use super::{Sut, Terminal};

pub struct MysqlSut {
    connection: Connection,
}

impl MysqlSut {
    pub fn new(connection: Connection) -> Self {
        Self { connection }
    }

    fn build_options_for_schema(&self) -> MySqlConnectOptions {
        let mut mysql = MySqlConnectOptions::new()
            .username(&self.connection.schema_user.username)
            .password(&self.connection.schema_user.password)
            .database(&self.connection.database)
            .host(&self.connection.host)
            .disable_statement_logging();
        if let Some(p) = self.connection.port {
            mysql = mysql.port(p);
        }
        mysql
    }
}

#[async_trait]
impl Sut for MysqlSut {
    async fn terminal(&self, _id: u32) -> Result<Box<dyn Terminal>, sqlx::Error> {
        todo!()
    }

    #[instrument(skip(self))]
    async fn build_schema(&self) -> Result<(), sqlx::Error> {
        let mut conn = self.build_options_for_schema().connect().await?;
        #[rustfmt::skip]
        let sql_set = [
"SET FOREIGN_KEY_CHECKS = 0",
r#"CREATE TABLE `warehouse` (
  `w_id` INT(6) NOT NULL,
  `w_ytd` DECIMAL(12, 2) NULL,
  `w_tax` DECIMAL(4, 4) NULL,
  `w_name` VARCHAR(10) BINARY NULL,
  `w_street_1` VARCHAR(20) BINARY NULL,
  `w_street_2` VARCHAR(20) BINARY NULL,
  `w_city` VARCHAR(20) BINARY NULL,
  `w_state` CHAR(2) BINARY NULL,
  `w_zip` CHAR(9) BINARY NULL,
PRIMARY KEY (`w_id`)
)"#,

r#"
CREATE TABLE `district` (
  `d_id` INT(2) NOT NULL,
  `d_w_id` INT(6) NOT NULL,
  `d_ytd` DECIMAL(12, 2) NULL,
  `d_tax` DECIMAL(4, 4) NULL,
  `d_next_o_id` INT NULL,
  `d_name` VARCHAR(10) BINARY NULL,
  `d_street_1` VARCHAR(20) BINARY NULL,
  `d_street_2` VARCHAR(20) BINARY NULL,
  `d_city` VARCHAR(20) BINARY NULL,
  `d_state` CHAR(2) BINARY NULL,
  `d_zip` CHAR(9) BINARY NULL,
PRIMARY KEY (`d_w_id`,`d_id`)
)"#,

r#"
CREATE TABLE `customer` (
  `c_id` INT(5) NOT NULL,
  `c_d_id` INT(2) NOT NULL,
  `c_w_id` INT(6) NOT NULL,
  `c_first` VARCHAR(16) BINARY NULL,
  `c_middle` CHAR(2) BINARY NULL,
  `c_last` VARCHAR(16) BINARY NULL,
  `c_street_1` VARCHAR(20) BINARY NULL,
  `c_street_2` VARCHAR(20) BINARY NULL,
  `c_city` VARCHAR(20) BINARY NULL,
  `c_state` CHAR(2) BINARY NULL,
  `c_zip` CHAR(9) BINARY NULL,
  `c_phone` CHAR(16) BINARY NULL,
  `c_since` DATETIME NULL,
  `c_credit` CHAR(2) BINARY NULL,
  `c_credit_lim` DECIMAL(12, 2) NULL,
  `c_discount` DECIMAL(4, 4) NULL,
  `c_balance` DECIMAL(12, 2) NULL,
  `c_ytd_payment` DECIMAL(12, 2) NULL,
  `c_payment_cnt` INT(8) NULL,
  `c_delivery_cnt` INT(8) NULL,
  `c_data` VARCHAR(500) BINARY NULL,
PRIMARY KEY (`c_w_id`,`c_d_id`,`c_id`),
KEY c_w_id (`c_w_id`,`c_d_id`,`c_last`(16),`c_first`(16))
)"#,

r#"CREATE TABLE `history` (
  `h_c_id` INT NULL,
  `h_c_d_id` INT NULL,
  `h_c_w_id` INT NULL,
  `h_d_id` INT NULL,
  `h_w_id` INT NULL,
  `h_date` DATETIME NULL,
  `h_amount` DECIMAL(6, 2) NULL,
  `h_data` VARCHAR(24) BINARY NULL
)
"#,

r#"
CREATE TABLE `new_order` (
  `no_w_id` INT NOT NULL,
  `no_d_id` INT NOT NULL,
  `no_o_id` INT NOT NULL,
PRIMARY KEY (`no_w_id`, `no_d_id`, `no_o_id`)
)"#,

r#"
CREATE TABLE `oorder` (
  `o_id` INT NOT NULL,
  `o_w_id` INT NOT NULL,
  `o_d_id` INT NOT NULL,
  `o_c_id` INT NULL,
  `o_carrier_id` INT NULL,
  `o_ol_cnt` INT NULL,
  `o_all_local` INT NULL,
  `o_entry_d` DATETIME NULL,
PRIMARY KEY (`o_w_id`,`o_d_id`,`o_id`),
KEY o_w_id (`o_w_id`,`o_d_id`,`o_c_id`,`o_id`)
)"#,

r#"
CREATE TABLE `order_line` (
  `ol_w_id` INT NOT NULL,
  `ol_d_id` INT NOT NULL,
  `ol_o_id` iNT NOT NULL,
  `ol_number` INT NOT NULL,
  `ol_i_id` INT NULL,
  `ol_delivery_d` DATETIME NULL,
  `ol_amount` INT NULL,
  `ol_supply_w_id` INT NULL,
  `ol_quantity` INT NULL,
  `ol_dist_info` CHAR(24) BINARY NULL,
PRIMARY KEY (`ol_w_id`,`ol_d_id`,`ol_o_id`,`ol_number`)
)"#,

r#"
CREATE TABLE `item` (
  `i_id` INT(6) NOT NULL,
  `i_im_id` INT NULL,
  `i_name` VARCHAR(24) BINARY NULL,
  `i_price` DECIMAL(5, 2) NULL,
  `i_data` VARCHAR(50) BINARY NULL,
PRIMARY KEY (`i_id`)
)"#,

r#"
CREATE TABLE `stock` (
  `s_i_id` INT(6) NOT NULL,
  `s_w_id` INT(6) NOT NULL,
  `s_quantity` INT(6) NULL,
  `s_dist_01` CHAR(24) BINARY NULL,
  `s_dist_02` CHAR(24) BINARY NULL,
  `s_dist_03` CHAR(24) BINARY NULL,
  `s_dist_04` CHAR(24) BINARY NULL,
  `s_dist_05` CHAR(24) BINARY NULL,
  `s_dist_06` CHAR(24) BINARY NULL,
  `s_dist_07` CHAR(24) BINARY NULL,
  `s_dist_08` CHAR(24) BINARY NULL,
  `s_dist_09` CHAR(24) BINARY NULL,
  `s_dist_10` CHAR(24) BINARY NULL,
  `s_ytd` BIGINT(10) NULL,
  `s_order_cnt` INT(6) NULL,
  `s_remote_cnt` INT(6) NULL,
  `s_data` VARCHAR(50) BINARY NULL,
PRIMARY KEY (`s_w_id`,`s_i_id`)
)"#,
];
        for sql in sql_set {
            conn.execute(sql).await?;
        }
        Ok(())
    }

    async fn after_loaded(&self) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn destroy_schema(&self) -> Result<(), sqlx::Error> {
        let mut conn = self.build_options_for_schema().connect().await?;
        // Drop order must be promised due to FOREIGN_KEY_CHECKS.
        for table in [
            "new_order",
            "order_line",
            "oorder",
            "history",
            "customer",
            "stock",
            "item",
            "district",
            "warehouse",
        ] {
            let sql = format!("drop table `{table}`;");
            conn.execute(sql.as_str()).await?;
        }
        Ok(())
    }

    async fn loader(&self) -> Result<Box<dyn Loader>, sqlx::Error> {
        Ok(Box::new(MysqlLoader::new(
            self.build_options_for_schema().connect().await?,
        )))
    }
}
