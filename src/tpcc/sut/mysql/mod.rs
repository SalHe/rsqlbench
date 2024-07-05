mod loader;

use crate::cfg::Connection;
use crate::tpcc::loader::Loader;
use async_trait::async_trait;
use loader::MysqlLoader;
use sqlx::mysql::MySqlConnectOptions;

use sqlx::{ConnectOptions, Executor, MySqlConnection};
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

    async fn build_fkeys(&self, conn: &mut MySqlConnection) -> Result<(), sqlx::Error> {
        let fkeys = [
            "alter table district add constraint d_warehouse_fkey foreign key (d_w_id) references warehouse (w_id);",
            "alter table customer add constraint c_district_fkey foreign key (c_w_id, c_d_id) references district (d_w_id, d_id); ",
            "alter table history add constraint h_customer_fkey foreign key (h_c_w_id, h_c_d_id, h_c_id) references customer (c_w_id, c_d_id, c_id);",
            "alter table history add constraint h_district_fkey foreign key (h_w_id, h_d_id) references district (d_w_id, d_id);",
            "alter table new_order add constraint no_order_fkey foreign key (no_w_id, no_d_id, no_o_id) references oorder (o_w_id, o_d_id, o_id); ",
            "alter table oorder add constraint o_customer_fkey foreign key (o_w_id, o_d_id, o_c_id) references customer (c_w_id, c_d_id, c_id); ",
            "alter table order_line add constraint ol_order_fkey foreign key (ol_w_id, ol_d_id, ol_o_id) references oorder (o_w_id, o_d_id, o_id); ",
            "alter table order_line add constraint ol_stock_fkey foreign key (ol_supply_w_id, ol_i_id) references stock (s_w_id, s_i_id); ",
            "alter table stock add constraint s_warehouse_fkey foreign key (s_w_id) references warehouse (w_id); ",
            "alter table stock add constraint s_item_fkey foreign key (s_i_id) references item (i_id);"
        ];
        for sql in fkeys {
            conn.execute(sql).await?;
        }
        Ok(())
    }

    async fn build_indexes(&self, conn: &mut MySqlConnection) -> Result<(), sqlx::Error> {
        let ddl = [
            "alter table warehouse add constraint warehouse_pkey primary key (w_id);",
            "alter table district add constraint district_pkey primary key (d_w_id, d_id);",
            "alter table customer add constraint customer_pkey primary key (c_w_id, c_d_id, c_id);",
            "create index customer_idx1 on  customer (c_w_id, c_d_id, c_last, c_first);",
            "alter table oorder add constraint oorder_pkey primary key (o_w_id, o_d_id, o_id);",
            "create unique index oorder_idx1 on  oorder (o_w_id, o_d_id, o_carrier_id, o_id);",
            "alter table new_order add constraint new_order_pkey primary key (no_w_id, no_d_id, no_o_id);",
            "alter table order_line add constraint order_line_pkey primary key (ol_w_id, ol_d_id, ol_o_id, ol_number);",
            "alter table stock add constraint stock_pkey primary key (s_w_id, s_i_id);",
            "alter table item add constraint item_pkey primary key (i_id);",
        ];
        for sql in ddl {
            conn.execute(sql).await?;
        }
        Ok(())
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
        let tables = [
r#"
create table warehouse (
  w_id        integer   not null,
  w_ytd       decimal(12,2),
  w_tax       decimal(4,4),
  w_name      varchar(10),
  w_street_1  varchar(20),
  w_street_2  varchar(20),
  w_city      varchar(20),
  w_state     char(2),
  w_zip       char(9)
);
"#,

r#"
create table district (
  d_w_id       integer       not null,
  d_id         integer       not null,
  d_ytd        decimal(12,2),
  d_tax        decimal(4,4),
  d_next_o_id  integer,
  d_name       varchar(10),
  d_street_1   varchar(20),
  d_street_2   varchar(20),
  d_city       varchar(20),
  d_state      char(2),
  d_zip        char(9)
);"#,

r#"
create table customer (
  c_w_id         integer        not null,
  c_d_id         integer        not null,
  c_id           integer        not null,
  c_discount     decimal(4,4),
  c_credit       char(2),
  c_last         varchar(16),
  c_first        varchar(16),
  c_credit_lim   decimal(12,2),
  c_balance      decimal(12,2),
  c_ytd_payment  decimal(12,2),
  c_payment_cnt  integer,
  c_delivery_cnt integer,
  c_street_1     varchar(20),
  c_street_2     varchar(20),
  c_city         varchar(20),
  c_state        char(2),
  c_zip          char(9),
  c_phone        char(16),
  c_since        timestamp,
  c_middle       char(2),
  c_data         varchar(500)
);
"#,

r#"create table history (
  h_c_id   integer,
  h_c_d_id integer,
  h_c_w_id integer,
  h_d_id   integer,
  h_w_id   integer,
  h_date   timestamp,
  h_amount decimal(6,2),
  h_data   varchar(24)
);
"#,

r#"
create table new_order (
  no_w_id  integer   not null,
  no_d_id  integer   not null,
  no_o_id  integer   not null
);
"#,

r#"
create table oorder (
  o_w_id       integer      not null,
  o_d_id       integer      not null,
  o_id         integer      not null,
  o_c_id       integer,
  o_carrier_id integer,
  o_ol_cnt     integer,
  o_all_local  integer,
  o_entry_d    timestamp
);
"#,

r#"
create table order_line (
  ol_w_id         integer   not null,
  ol_d_id         integer   not null,
  ol_o_id         integer   not null,
  ol_number       integer   not null,
  ol_i_id         integer   not null,
  ol_delivery_d   timestamp,
  ol_amount       decimal(6,2),
  ol_supply_w_id  integer,
  ol_quantity     integer,
  ol_dist_info    char(24)
);
"#,

r#"
create table item (
  i_id     integer      not null,
  i_name   varchar(24),
  i_price  decimal(5,2),
  i_data   varchar(50),
  i_im_id  integer
);
"#,

r#"
create table stock (
  s_w_id       integer       not null,
  s_i_id       integer       not null,
  s_quantity   integer,
  s_ytd        integer,
  s_order_cnt  integer,
  s_remote_cnt integer,
  s_data       varchar(50),
  s_dist_01    char(24),
  s_dist_02    char(24),
  s_dist_03    char(24),
  s_dist_04    char(24),
  s_dist_05    char(24),
  s_dist_06    char(24),
  s_dist_07    char(24),
  s_dist_08    char(24),
  s_dist_09    char(24),
  s_dist_10    char(24)
);
"#,
];
        for sql in tables {
            conn.execute(sql).await?;
        }
        Ok(())
    }

    async fn after_loaded(&self) -> Result<(), sqlx::Error> {
        let mut conn = self.build_options_for_schema().connect().await?;
        self.build_indexes(&mut conn).await?;
        self.build_fkeys(&mut conn).await?;
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
