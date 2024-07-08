use sqlx::{
    database::{HasArguments, HasStatement},
    Database, Encode, Executor, IntoArguments, Statement, Type,
};
use static_assertions::const_assert;
use time::OffsetDateTime;
use tracing::info;

use crate::tpcc::model::{
    CustomerGenerator, DistrictGenerator, HistoryGenerator, OrderGenerator, OrderLineGenerator,
    StockGenerator, CUSTOMER_PER_DISTRICT, DISTRICT_PER_WAREHOUSE, ORDERS_PER_DISTRICT,
    STOCKS_PER_WAREHOUSE,
};

use super::model::{District, ItemGenerator, Warehouse};

#[async_trait::async_trait]
pub trait Loader: Send {
    async fn load_items(&mut self, generator: ItemGenerator) -> Result<(), sqlx::Error>;
    async fn load_warehouses(
        &mut self,
        generator: async_channel::Receiver<Warehouse>,
    ) -> Result<(), sqlx::Error>;
}

pub async fn load_stocks<DB>(
    warehouse: &Warehouse,
    txn: &mut sqlx::Transaction<'_, DB>,
) -> Result<(), sqlx::Error>
where
    DB: Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'a> <DB as HasStatement<'a>>::Statement: Statement<'a, Database = DB>,
    for<'a> u32: Encode<'a, DB> + Type<DB>,
    for<'a> u16: Encode<'a, DB> + Type<DB>,
    for<'a> String: Encode<'a, DB> + Type<DB>,
{
    const BATCH_SIZE: usize = 1000;
    info!(
        "Loading stocks for warehouse ID={id} (batch size={batch_size})",
        id = warehouse.id,
        batch_size = BATCH_SIZE
    );
    const BATCH_SQL: &str = const_format::concatcp!("INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES ", const_format::str_repeat!(
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),",
        BATCH_SIZE
    ));
    const_assert!(STOCKS_PER_WAREHOUSE % BATCH_SIZE == 0);

    let stmt = txn.prepare(&BATCH_SQL[0..BATCH_SQL.len() - 1]).await?;
    let mut query = stmt.query();
    for stock in StockGenerator::from_warehouse(warehouse) {
        query = query.bind(stock.item_id);
        query = query.bind(stock.warehouse_id);
        query = query.bind(stock.quantity);
        query = query.bind(stock.dist[0].clone());
        query = query.bind(stock.dist[1].clone());
        query = query.bind(stock.dist[2].clone());
        query = query.bind(stock.dist[3].clone());
        query = query.bind(stock.dist[4].clone());
        query = query.bind(stock.dist[5].clone());
        query = query.bind(stock.dist[6].clone());
        query = query.bind(stock.dist[7].clone());
        query = query.bind(stock.dist[8].clone());
        query = query.bind(stock.dist[9].clone());
        query = query.bind(stock.ytd);
        query = query.bind(stock.order_count);
        query = query.bind(stock.remote_count);
        query = query.bind(stock.data.clone());
        if stock.item_id % (BATCH_SIZE as u32) == 0 {
            query.execute(&mut **txn).await?;
            query = stmt.query();
        }
    }
    Ok(())
}

pub async fn load_warehouse<DB>(
    warehouse: &Warehouse,
    txn: &mut sqlx::Transaction<'_, DB>,
) -> Result<(), sqlx::Error>
where
    DB: Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'a> <DB as HasStatement<'a>>::Statement: Statement<'a, Database = DB>,
    for<'a> u8: Encode<'a, DB> + Type<DB>,
    for<'a> u16: Encode<'a, DB> + Type<DB>,
    for<'a> u32: Encode<'a, DB> + Type<DB>,
    for<'a> f32: Encode<'a, DB> + Type<DB>,
    for<'a> f64: Encode<'a, DB> + Type<DB>,
    for<'a> bool: Encode<'a, DB> + Type<DB>,
    for<'a> String: Encode<'a, DB> + Type<DB>,
    for<'a> Option<u8>: Encode<'a, DB> + Type<DB>,
    for<'a> OffsetDateTime: Encode<'a, DB> + Type<DB>,
{
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

async fn load_districts<DB>(
    warehouse: &Warehouse,
    txn: &mut sqlx::Transaction<'_, DB>,
) -> Result<(), sqlx::Error>
where
    DB: Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'a> <DB as HasStatement<'a>>::Statement: Statement<'a, Database = DB>,
    for<'a> u8: Encode<'a, DB> + Type<DB>,
    for<'a> u16: Encode<'a, DB> + Type<DB>,
    for<'a> u32: Encode<'a, DB> + Type<DB>,
    for<'a> f32: Encode<'a, DB> + Type<DB>,
    for<'a> f64: Encode<'a, DB> + Type<DB>,
    for<'a> bool: Encode<'a, DB> + Type<DB>,
    for<'a> String: Encode<'a, DB> + Type<DB>,
    for<'a> Option<u8>: Encode<'a, DB> + Type<DB>,
    for<'a> OffsetDateTime: Encode<'a, DB> + Type<DB>,
{
    const BATCH_SIZE: usize = DISTRICT_PER_WAREHOUSE; // There are several districts for single warehouse.
    info!(
        "Loading districts for warehouse ID={id} (batch size={batch_size})",
        id = warehouse.id,
        batch_size = BATCH_SIZE
    );
    const BATCH_SQL: &str = const_format::concatcp!("INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES ", const_format::str_repeat!(
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),",
        BATCH_SIZE
    ));
    const_assert!(DISTRICT_PER_WAREHOUSE % BATCH_SIZE == 0);

    let stmt = txn.prepare(&BATCH_SQL[0..BATCH_SQL.len() - 1]).await?;
    let mut query = stmt.query();
    for district in DistrictGenerator::from_warehouse(warehouse) {
        query = query.bind(district.id);
        query = query.bind(district.warehouse_id);
        query = query.bind(district.name.clone());
        query = query.bind(district.street.0.clone());
        query = query.bind(district.street.1.clone());
        query = query.bind(district.city.clone());
        query = query.bind(district.state.clone());
        query = query.bind(district.zip.clone());
        query = query.bind(district.tax);
        query = query.bind(district.ytd);
        query = query.bind(district.next_order_id);
        if district.id % (BATCH_SIZE as u8) == 0 {
            query.execute(&mut **txn).await?;
            query = stmt.query();
        }
        load_customers(&district, txn).await?;
        load_orders(&district, txn).await?;
    }
    Ok(())
}

async fn load_customers<DB>(
    district: &District,
    txn: &mut sqlx::Transaction<'_, DB>,
) -> Result<(), sqlx::Error>
where
    DB: Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'a> <DB as HasStatement<'a>>::Statement: Statement<'a, Database = DB>,
    for<'a> u8: Encode<'a, DB> + Type<DB>,
    for<'a> u16: Encode<'a, DB> + Type<DB>,
    for<'a> u32: Encode<'a, DB> + Type<DB>,
    for<'a> f32: Encode<'a, DB> + Type<DB>,
    for<'a> f64: Encode<'a, DB> + Type<DB>,
    for<'a> String: Encode<'a, DB> + Type<DB>,
    for<'a> OffsetDateTime: Encode<'a, DB> + Type<DB>,
{
    info!(
        "Loading customers for districts ID={d_id} for warehouse ID={id} (batch size={batch_size})",
        d_id = district.id,
        id = district.warehouse_id,
        batch_size = BATCH_SIZE
    );
    const BATCH_SIZE: usize = 1000; // There are several districts for single warehouse.
    const BATCH_SQL: &str = const_format::concatcp!("INSERT INTO customer ( c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) VALUES", const_format::str_repeat!(
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),",
        BATCH_SIZE
    ));
    const_assert!(CUSTOMER_PER_DISTRICT % BATCH_SIZE == 0);
    let stmt_customer = txn.prepare(&BATCH_SQL[0..BATCH_SQL.len() - 1]).await?;
    let stmt_history = txn.prepare("INSERT INTO history (  h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)").await?;
    let mut query = stmt_customer.query();
    for customer in CustomerGenerator::from_district(district) {
        query = query.bind(customer.id);
        query = query.bind(customer.district_id);
        query = query.bind(customer.warehouse_id);
        query = query.bind(customer.first_name.clone());
        query = query.bind(customer.middle_name.clone());
        query = query.bind(customer.last_name.clone());
        query = query.bind(customer.street.0.clone());
        query = query.bind(customer.street.1.clone());
        query = query.bind(customer.city.clone());
        query = query.bind(customer.state.clone());
        query = query.bind(customer.zip.clone());
        query = query.bind(customer.phone.clone());
        query = query.bind(
            customer
                .since
                .unwrap_or(OffsetDateTime::now_local().unwrap()),
        );
        query = query.bind(customer.credit.clone());
        query = query.bind(customer.credit_limit);
        query = query.bind(customer.discount);
        query = query.bind(customer.balance);
        query = query.bind(customer.ytd_payment);
        query = query.bind(customer.payment_count);
        query = query.bind(customer.delivery_count);
        query = query.bind(customer.data.clone());
        if customer.id % (BATCH_SIZE as u16) == 0 {
            query.execute(&mut **txn).await?;
            query = stmt_customer.query();
        }

        for history in HistoryGenerator::from_customer(&customer) {
            // single
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

async fn load_orders<DB>(
    district: &District,
    txn: &mut sqlx::Transaction<'_, DB>,
) -> Result<(), sqlx::Error>
where
    DB: Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'a> <DB as HasStatement<'a>>::Statement: Statement<'a, Database = DB>,
    for<'a> u8: Encode<'a, DB> + Type<DB>,
    for<'a> u16: Encode<'a, DB> + Type<DB>,
    for<'a> u32: Encode<'a, DB> + Type<DB>,
    for<'a> f32: Encode<'a, DB> + Type<DB>,
    for<'a> f64: Encode<'a, DB> + Type<DB>,
    for<'a> bool: Encode<'a, DB> + Type<DB>,
    for<'a> String: Encode<'a, DB> + Type<DB>,
    for<'a> OffsetDateTime: Encode<'a, DB> + Type<DB>,
    for<'a> Option<u8>: Encode<'a, DB> + Type<DB>,
{
    info!(
        "Loading orders for districts ID={d_id} for warehouse ID={id} (batch size = {batch_size})",
        d_id = district.id,
        id = district.warehouse_id,
        batch_size = BATCH_SIZE
    );
    const BATCH_SIZE: usize = 1000;
    const BATCH_SQL: &str = const_format::concatcp!("INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES", const_format::str_repeat!(
        "(?, ?, ?, ?, ?, ?, ?, ?),",
        BATCH_SIZE
    ));
    const_assert!(ORDERS_PER_DISTRICT % BATCH_SIZE == 0);
    let stmt_order = txn.prepare(&BATCH_SQL[0..BATCH_SQL.len() - 1]).await?;
    let mut query_order = stmt_order.query();

    let stmt_new_order = txn
        .prepare("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)")
        .await?;

    for (order, new_order) in OrderGenerator::from_district(district) {
        query_order = query_order.bind(order.id);
        query_order = query_order.bind(order.district_id);
        query_order = query_order.bind(order.warehouse_id);
        query_order = query_order.bind(order.customer_id);
        query_order = query_order.bind(
            order
                .entry_date
                .unwrap_or(OffsetDateTime::now_local().unwrap()),
        );
        query_order = query_order.bind(order.carrier_id);
        query_order = query_order.bind(order.order_lines_count);
        query_order = query_order.bind(order.all_local);
        if order.id % (BATCH_SIZE as u32) == 0 {
            query_order.execute(&mut **txn).await?;
            query_order = stmt_order.query();
        }

        // TODO batch optimize
        if let Some(new_order) = new_order {
            stmt_new_order
                .query()
                .bind(new_order.order_id)
                .bind(new_order.district_id)
                .bind(new_order.warehouse_id)
                .execute(&mut **txn)
                .await?;
        }

        // Insert order lines.
        let stmt_order_line_sql = {
            const PREFIX: &str="INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES";
            const VLIST: &str = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?),";
            let mut sql = String::with_capacity(
                PREFIX.len() + VLIST.len() * (order.order_lines_count as usize),
            );
            sql.push_str(PREFIX);
            sql.push_str(&VLIST.repeat(order.order_lines_count as usize));
            sql
        };
        let stmt_order_line = txn
            .prepare(&stmt_order_line_sql[0..stmt_order_line_sql.len() - 1])
            .await?;
        let mut query_order_lines = stmt_order_line.query();
        for ol in OrderLineGenerator::from_order(&order) {
            query_order_lines = query_order_lines.bind(ol.order_id);
            query_order_lines = query_order_lines.bind(ol.district_id);
            query_order_lines = query_order_lines.bind(ol.warehouse_id);
            query_order_lines = query_order_lines.bind(ol.number);
            query_order_lines = query_order_lines.bind(ol.item_id);
            query_order_lines = query_order_lines.bind(ol.supply_warehouse_id);
            query_order_lines = query_order_lines.bind(
                ol.delivery_date
                    .unwrap_or(OffsetDateTime::now_local().unwrap()),
            );
            query_order_lines = query_order_lines.bind(ol.quantity);
            query_order_lines = query_order_lines.bind(ol.amount);
            query_order_lines = query_order_lines.bind(ol.dist_info);
        }
        query_order_lines.execute(&mut **txn).await?;
    }
    Ok(())
}

pub async fn load_items<DB>(
    generator: ItemGenerator,
    txn: &mut sqlx::Transaction<'_, DB>,
) -> Result<(), sqlx::Error>
where
    DB: Database,
    for<'a> &'a mut DB::Connection: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'a> <DB as HasStatement<'a>>::Statement: Statement<'a, Database = DB>,
    for<'a> u32: Encode<'a, DB> + Type<DB>,
    for<'a> u16: Encode<'a, DB> + Type<DB>,
    for<'a> f32: Encode<'a, DB> + Type<DB>,
    for<'a> String: Encode<'a, DB> + Type<DB>,
{
    const BATCH_SIZE: usize = 100;
    info!(
        "Loading items (batch size={batch_size})",
        batch_size = BATCH_SIZE
    );
    const BATCH_SQL: &str = const_format::concatcp!(
        "INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) VALUES",
        const_format::str_repeat!("(?, ?, ?, ?, ?),", BATCH_SIZE)
    );
    const_assert!(STOCKS_PER_WAREHOUSE % BATCH_SIZE == 0);

    let stmt = txn.prepare(&BATCH_SQL[0..BATCH_SQL.len() - 1]).await?;
    let mut query = stmt.query();
    for (idx, item) in generator.enumerate() {
        query = query.bind(item.id);
        query = query.bind(item.image_id);
        query = query.bind(item.name.clone());
        query = query.bind(item.price);
        query = query.bind(item.data.clone());
        if (idx + 1) % BATCH_SIZE == 0 {
            query.execute(&mut **txn).await?;
            query = stmt.query();
        }
    }
    Ok(())
}
