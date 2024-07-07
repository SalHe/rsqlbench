use async_trait::async_trait;
use sqlx::{prelude::*, MySqlConnection};
use time::OffsetDateTime;
use tracing::{info, instrument};

use crate::tpcc::{
    loader::Loader,
    model::{
        CustomerGenerator, District, DistrictGenerator, HistoryGenerator, ItemGenerator,
        OrderGenerator, OrderLineGenerator, StockGenerator, Warehouse,
    },
};

pub struct MysqlLoader {
    conn: MySqlConnection,
}

impl MysqlLoader {
    pub fn new(conn: MySqlConnection) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl Loader for MysqlLoader {
    #[instrument(skip(self, generator))]
    async fn load_items(&mut self, generator: ItemGenerator) -> Result<(), sqlx::Error> {
        self.conn
            .transaction(|txn| {
                Box::pin(async move {
                    let stmt = txn.prepare("INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) VALUES (?, ?, ?, ?, ?)").await?;
                    for item in generator {
                        if item.id % 1000 == 1 {
                            info!("Loading items from ID={id} to ID={end_id}", id=item.id, end_id=item.id+1000);
                        }
                        stmt.query()
                            .bind(item.id)
                            .bind(item.image_id)
                            .bind(item.name)
                            .bind(item.price)
                            .bind(item.data)
                            .execute(&mut **txn)
                            .await?;
                    }
                    let ok: Result<(), sqlx::Error> = Ok(());
                    ok
                })
            })
            .await
    }

    #[instrument(skip(self, generator))]
    async fn load_warehouses(
        &mut self,
        generator: async_channel::Receiver<Warehouse>,
    ) -> Result<(), sqlx::Error> {
        while let Ok(warehouse) = generator.recv().await {
            info!("Loading warehouse ID={id}", id = warehouse.id);
            self.conn
                .transaction(|txn| Box::pin(async move { load_warehouse(&warehouse, txn).await }))
                .await?;
        }
        Ok(())
    }
}

async fn load_stocks(
    warehouse: &Warehouse,
    txn: &mut sqlx::Transaction<'_, sqlx::MySql>,
) -> Result<(), sqlx::Error> {
    info!("Loading stocks for warehouse ID={id}", id = warehouse.id);
    let stmt = txn
        .prepare("INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .await?;
    for stock in StockGenerator::from_warehouse(warehouse) {
        stmt.query()
            .bind(stock.item_id)
            .bind(stock.warehouse_id)
            .bind(stock.quantity)
            .bind(&stock.dist[0])
            .bind(&stock.dist[1])
            .bind(&stock.dist[2])
            .bind(&stock.dist[3])
            .bind(&stock.dist[4])
            .bind(&stock.dist[5])
            .bind(&stock.dist[6])
            .bind(&stock.dist[7])
            .bind(&stock.dist[8])
            .bind(&stock.dist[9])
            .bind(stock.ytd)
            .bind(stock.order_count)
            .bind(stock.remote_count)
            .bind(&stock.data)
            .execute(&mut **txn)
            .await?;
    }
    Ok(())
}

async fn load_customers(
    district: &District,
    txn: &mut sqlx::Transaction<'_, sqlx::MySql>,
) -> Result<(), sqlx::Error> {
    info!(
        "Loading customers for districts ID={d_id} for warehouse ID={id}",
        d_id = district.id,
        id = district.warehouse_id
    );
    let stmt_customer = txn.prepare("INSERT INTO customer ( c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt,   c_delivery_cnt, c_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,         ?, ?, ?, ?, ?, ?)").await?;
    let stmt_history = txn.prepare("INSERT INTO history (  h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)").await?;
    for customer in CustomerGenerator::from_district(district) {
        stmt_customer
            .query()
            .bind(customer.id)
            .bind(customer.district_id)
            .bind(customer.warehouse_id)
            .bind(&customer.first_name)
            .bind(&customer.middle_name)
            .bind(&customer.last_name)
            .bind(&customer.street.0)
            .bind(&customer.street.1)
            .bind(&customer.city)
            .bind(&customer.state)
            .bind(&customer.zip)
            .bind(&customer.phone)
            .bind(
                customer
                    .since
                    .unwrap_or(OffsetDateTime::now_local().unwrap()),
            )
            .bind(&customer.credit)
            .bind(customer.credit_limit)
            .bind(customer.discount)
            .bind(customer.balance)
            .bind(customer.ytd_payment)
            .bind(customer.payment_count)
            .bind(customer.delivery_count)
            .bind(&customer.data)
            .execute(&mut **txn)
            .await?;

        for history in HistoryGenerator::from_customer(&customer) {
            stmt_history
                .query()
                .bind(history.customer_id)
                .bind(history.customer_district_id)
                .bind(history.customer_warehouse_id)
                .bind(history.district_id)
                .bind(history.warehouse_id)
                .bind(history.date.unwrap_or(OffsetDateTime::now_local().unwrap()))
                .bind(history.amount)
                .bind(&history.data)
                .execute(&mut **txn)
                .await?;
        }
    }
    Ok(())
}

async fn load_districts(
    warehouse: &Warehouse,
    txn: &mut sqlx::Transaction<'_, sqlx::MySql>,
) -> Result<(), sqlx::Error> {
    info!("Loading districts for warehouse ID={id}", id = warehouse.id);
    let stmt = txn.prepare("INSERT INTO district (  d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").await?;
    for district in DistrictGenerator::from_warehouse(warehouse) {
        stmt.query()
            .bind(district.id)
            .bind(district.warehouse_id)
            .bind(&district.name)
            .bind(&district.street.0)
            .bind(&district.street.1)
            .bind(&district.city)
            .bind(&district.state)
            .bind(&district.zip)
            .bind(district.tax)
            .bind(district.ytd)
            .bind(district.next_order_id)
            .execute(&mut **txn)
            .await?;
        load_customers(&district, txn).await?;
        load_orders(&district, txn).await?;
    }
    Ok(())
}
async fn load_warehouse(
    warehouse: &Warehouse,
    txn: &mut sqlx::Transaction<'_, sqlx::MySql>,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                            .bind(warehouse.id)
                            .bind(&warehouse.name)
                            .bind(&warehouse.street.0)
                            .bind(&warehouse.street.1)
                            .bind(&warehouse.city)
                            .bind(&warehouse.state)
                            .bind(&warehouse.zip)
                            .bind(warehouse.tax)
                            .bind(warehouse.ytd)
                            .execute(&mut **txn)
                            .await?;
    load_stocks(warehouse, txn).await?;
    load_districts(warehouse, txn).await?;
    Ok(())
}

async fn load_orders(
    district: &District,
    txn: &mut sqlx::Transaction<'_, sqlx::MySql>,
) -> Result<(), sqlx::Error> {
    info!(
        "Loading orders for districts ID={d_id} for warehouse ID={id}",
        d_id = district.id,
        id = district.warehouse_id
    );
    let stmt_order = txn.prepare("INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?, ?)").await?;
    let stmt_new_order = txn
        .prepare("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)")
        .await?;
    let stmt_order_line = txn.prepare("INSERT INTO order_line (  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity,   ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").await?;

    for (order, new_order) in OrderGenerator::from_district(district) {
        stmt_order
            .query()
            .bind(order.id)
            .bind(order.district_id)
            .bind(order.warehouse_id)
            .bind(order.customer_id)
            .bind(
                order
                    .entry_date
                    .unwrap_or(OffsetDateTime::now_local().unwrap()),
            )
            .bind(order.carrier_id)
            .bind(order.order_lines_count)
            .bind(order.all_local)
            .execute(&mut **txn)
            .await?;
        if let Some(new_order) = new_order {
            stmt_new_order
                .query()
                .bind(new_order.order_id)
                .bind(new_order.district_id)
                .bind(new_order.warehouse_id)
                .execute(&mut **txn)
                .await?;
        }
        for ol in OrderLineGenerator::from_order(&order) {
            stmt_order_line
                .query()
                .bind(ol.order_id)
                .bind(ol.district_id)
                .bind(ol.warehouse_id)
                .bind(ol.number)
                .bind(ol.item_id)
                .bind(ol.supply_warehouse_id)
                .bind(
                    ol.delivery_date
                        .unwrap_or(OffsetDateTime::now_local().unwrap()),
                )
                .bind(ol.quantity)
                .bind(ol.amount)
                .bind(&ol.dist_info)
                .execute(&mut **txn)
                .await?;
        }
    }
    Ok(())
}
