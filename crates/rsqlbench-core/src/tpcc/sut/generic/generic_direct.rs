//! TODO merge direct.rs

use std::future::Future;

use sqlx::{database::HasArguments, Database, IntoArguments};
use tracing::info;

use crate::tpcc::model::{
    Customer, CustomerGenerator, District, DistrictGenerator, History, HistoryGenerator, Item,
    ItemGenerator, NewOrder, Order, OrderGenerator, OrderLine, OrderLineGenerator, Stock,
    StockGenerator, Warehouse, CUSTOMER_PER_DISTRICT, DISTRICT_PER_WAREHOUSE, ORDERS_PER_DISTRICT,
    STOCKS_PER_WAREHOUSE,
};

pub trait Executor {
    fn execute(&mut self, sql: &str) -> impl Future<Output = anyhow::Result<()>> + Send;
}

pub struct SqlxExecutorWrapper<'e, DB, E>
where
    DB: Database,
    for<'a> &'a mut E: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
{
    executor: &'e mut E,
}

impl<'e, DB, E> SqlxExecutorWrapper<'e, DB, E>
where
    DB: Database,
    for<'a> &'a mut E: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
{
    pub fn new(executor: &'e mut E) -> Self {
        Self { executor }
    }
}

impl<'e, DB, E> Executor for SqlxExecutorWrapper<'e, DB, E>
where
    DB: Database,
    for<'a> &'a mut E: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
{
    async fn execute(&mut self, sql: &str) -> anyhow::Result<()> {
        sqlx::query(sql).execute(&mut *self.executor).await?;
        Ok(())
    }
}

pub async fn load_stocks(
    warehouse: &Warehouse,
    executor: &mut impl Executor,
    batch_size: usize,
) -> anyhow::Result<()> {
    info!(
        "Loading stocks for warehouse ID={id} (batch size={batch_size})",
        id = warehouse.id
    );
    assert!(STOCKS_PER_WAREHOUSE % batch_size == 0);

    const SQL: &str = "INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES ";
    let mut sql = SQL.to_string();
    for stock in StockGenerator::from_warehouse(warehouse) {
        let Stock {
            item_id,
            warehouse_id,
            quantity,
            dist: [dist0, dist1, dist2, dist3, dist4, dist5, dist6, dist7, dist8, dist9],
            ytd,
            order_count,
            remote_count,
            data,
        } = stock;
        sql.push_str(&format!("('{item_id}', '{warehouse_id}', '{quantity}', '{dist0}', '{dist1}', '{dist2}', '{dist3}', '{dist4}', '{dist5}', '{dist6}', '{dist7}', '{dist8}', '{dist9}', '{ytd}', '{order_count}', '{remote_count}', '{data}'),"));
        if stock.item_id % (batch_size as u32) == 0 {
            executor.execute(&sql[0..sql.len() - 1]).await?;
            sql.clear();
            sql.push_str(SQL);
        }
    }
    Ok(())
}

pub async fn load_warehouse(
    warehouse: &Warehouse,
    executor: &mut impl Executor,
) -> anyhow::Result<()> {
    let Warehouse {
        id,
        name,
        street: (street0, street1),
        city,
        state,
        zip,
        tax,
        ytd,
    } = warehouse;
    executor.execute(&format!("INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES ('{id}', '{name}', '{street0}', '{street1}', '{city}', '{state}', '{zip}', '{tax}', '{ytd}')"))
        .await?;
    load_stocks(warehouse, executor, 1000).await?;
    load_districts(warehouse, executor).await?;
    Ok(())
}

async fn load_districts(warehouse: &Warehouse, executor: &mut impl Executor) -> anyhow::Result<()> {
    let batch_size: usize = 10;
    info!(
        "Loading districts for warehouse ID={id} (batch size={batch_size})",
        id = warehouse.id,
    );
    const SQL: &str="INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES ";
    assert!(DISTRICT_PER_WAREHOUSE % batch_size == 0);

    let mut sql = SQL.to_string();
    for district in DistrictGenerator::from_warehouse(warehouse) {
        let District {
            id,
            warehouse_id,
            name,
            street: (street0, street1),
            city,
            state,
            zip,
            tax,
            ytd,
            next_order_id,
        } = &district;
        sql.push_str(&format!("('{id}','{warehouse_id}','{name}','{street0}','{street1}','{city}','{state}','{zip}','{tax}','{ytd}','{next_order_id}'),"));
        if district.id % (batch_size as u8) == 0 {
            executor.execute(&sql[0..sql.len() - 1]).await?;
            sql.clear();
            sql.push_str(SQL);
        }
        load_customers(&district, executor, CUSTOMER_PER_DISTRICT).await?;
        load_orders(&district, executor, ORDERS_PER_DISTRICT / 2).await?;
    }
    Ok(())
}

async fn load_customers(
    district: &District,
    executor: &mut impl Executor,
    batch_size: usize,
) -> anyhow::Result<()> {
    info!(
        "Loading customers for districts ID={d_id} for warehouse ID={id} (batch size={batch_size})",
        d_id = district.id,
        id = district.warehouse_id,
    );
    assert!(CUSTOMER_PER_DISTRICT % batch_size == 0);
    const CUSTOMER_SQL: &str = "INSERT INTO customer ( c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) VALUES";
    const HISTORY_SQL: &str = "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES ";

    let mut customer_sql = CUSTOMER_SQL.to_string();
    let mut history_sql = HISTORY_SQL.to_string();
    for customer in CustomerGenerator::from_district(district) {
        let Customer {
            id,
            district_id,
            warehouse_id,
            first_name,
            middle_name,
            last_name,
            street: (street0, street1),
            city,
            state,
            zip,
            phone,
            since: _,
            credit,
            credit_limit,
            discount,
            balance,
            ytd_payment,
            payment_count,
            delivery_count,
            data,
        } = &customer;
        customer_sql.push_str(&format!("('{id}', '{district_id}', '{warehouse_id}', '{first_name}', '{middle_name}', '{last_name}', '{street0}', '{street1}', '{city}', '{state}', '{zip}', '{phone}', NOW(), '{credit}', '{credit_limit}', '{discount}', '{balance}', '{ytd_payment}', '{payment_count}', '{delivery_count}', '{data}'),"));
        for history in HistoryGenerator::from_customer(&customer) {
            let History {
                customer_id,
                customer_district_id,
                customer_warehouse_id,
                district_id,
                warehouse_id,
                date: _,
                amount,
                data,
            } = history;
            history_sql.push_str(&format!("('{customer_id}', '{customer_district_id}', '{customer_warehouse_id}', '{district_id}', '{warehouse_id}', NOW(), '{amount}', '{data}'),"));
        }
        if customer.id % (batch_size as u16) == 0 {
            executor
                .execute(&customer_sql[0..customer_sql.len() - 1])
                .await?;
            executor
                .execute(&history_sql[0..history_sql.len() - 1])
                .await?;
            customer_sql.clear();
            history_sql.clear();
            customer_sql.push_str(CUSTOMER_SQL);
            history_sql.push_str(HISTORY_SQL);
        }
    }
    Ok(())
}

async fn load_orders(
    district: &District,
    executor: &mut impl Executor,
    batch_size: usize,
) -> anyhow::Result<()> {
    info!(
        "Loading orders for districts ID={d_id} for warehouse ID={id} (batch size = {batch_size})",
        d_id = district.id,
        id = district.warehouse_id,
    );
    const ORDER_SQL: &str = "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES";
    const NEW_ORDER_SQL: &str = "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ";
    const ORDER_LINE_SQL: &str = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES";
    assert!(ORDERS_PER_DISTRICT % batch_size == 0);
    let mut order_sql = ORDER_SQL.to_string();
    let mut new_order_sql = NEW_ORDER_SQL.to_string();
    let mut order_line_sql = ORDER_LINE_SQL.to_string();

    for (order, new_order) in OrderGenerator::from_district(district) {
        let Order {
            id,
            district_id,
            warehouse_id,
            customer_id,
            entry_date: _,
            carrier_id,
            order_lines_count,
            all_local,
        } = order;
        let carrier_id = carrier_id
            .map(|x| x.to_string())
            .unwrap_or("NULL".to_string());
        let all_local = if all_local { 1 } else { 0 };
        order_sql.push_str(&format!("('{id}', '{district_id}', '{warehouse_id}', '{customer_id}', NOW(), {carrier_id}, '{order_lines_count}', '{all_local}'),"));

        if let Some(new_order) = new_order {
            let NewOrder {
                order_id,
                district_id,
                warehouse_id,
            } = new_order;
            new_order_sql.push_str(&format!(
                "('{order_id}', '{district_id}', '{warehouse_id}'),",
            ));
        }

        for ol in OrderLineGenerator::from_order(&order) {
            let OrderLine {
                order_id,
                district_id,
                warehouse_id,
                number,
                item_id,
                supply_warehouse_id,
                delivery_date: _,
                quantity,
                amount,
                dist_info,
            } = ol;
            order_line_sql.push_str(&format!("('{order_id}', '{district_id}', '{warehouse_id}', '{number}', '{item_id}', '{supply_warehouse_id}', NOW(), '{quantity}', '{amount}', '{dist_info}'),"));
        }

        if order.id % (batch_size as u32) == 0 {
            executor.execute(&order_sql[0..order_sql.len() - 1]).await?;
            executor
                .execute(&order_line_sql[0..order_line_sql.len() - 1])
                .await?;

            order_sql.clear();
            order_line_sql.clear();
            order_sql.push_str(ORDER_SQL);
            order_line_sql.push_str(ORDER_LINE_SQL);

            if new_order_sql.ends_with(',') {
                executor
                    .execute(&new_order_sql[0..new_order_sql.len() - 1])
                    .await?;
                new_order_sql.clear();
                new_order_sql.push_str(NEW_ORDER_SQL);
            }
        }
    }
    Ok(())
}

pub async fn load_items(
    generator: ItemGenerator,
    batch_size: usize,
    executor: &mut impl Executor,
) -> anyhow::Result<()> {
    info!("Loading items (batch size={batch_size})");
    const SQL_PREFIX: &str = "INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) VALUES";
    assert!(STOCKS_PER_WAREHOUSE % batch_size == 0);

    let mut sql = SQL_PREFIX.to_string();
    for (idx, item) in generator.enumerate() {
        let Item {
            id,
            image_id,
            name,
            price,
            data,
        } = item;
        sql.push_str(&format!(
            "('{id}','{image_id}', '{name}', '{price}', '{data}'),"
        ));
        if (idx + 1) % batch_size == 0 {
            executor.execute(&sql[0..sql.len() - 1]).await?;
            sql.clear();
            sql.push_str(SQL_PREFIX);
        }
    }
    Ok(())
}
