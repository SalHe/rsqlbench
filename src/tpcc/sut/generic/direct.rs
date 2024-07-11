use sqlx::{database::HasArguments, Database, IntoArguments};
use tracing::info;

use crate::tpcc::model::{
    Customer, CustomerGenerator, District, DistrictGenerator, History, HistoryGenerator, Item,
    ItemGenerator, NewOrder, Order, OrderGenerator, OrderLine, OrderLineGenerator, Stock,
    StockGenerator, Warehouse, CUSTOMER_PER_DISTRICT, DISTRICT_PER_WAREHOUSE, ORDERS_PER_DISTRICT,
    STOCKS_PER_WAREHOUSE,
};

pub async fn load_stocks<DB, E>(
    warehouse: &Warehouse,
    conn: &mut E,
    batch_size: usize,
) -> anyhow::Result<()>
where
    DB: Database,
    for<'a> &'a mut E: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
{
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
            info!(
                "Executing insert stocks {stock_id} for warehouse ID={id} (batch size={batch_size})...",
                stock_id = stock.item_id,
                id = warehouse.id,
            );
            sqlx::query(&sql[0..sql.len() - 1])
                .execute(&mut *conn)
                .await?;
            info!(
                "Executed insert stocks {stock_id} for warehouse ID={id} (batch size={batch_size})",
                stock_id = stock.item_id,
                id = warehouse.id,
            );
            sql.clear();
            sql.push_str(SQL);
        }
    }
    Ok(())
}

pub async fn load_warehouse<DB, E>(warehouse: &Warehouse, conn: &mut E) -> anyhow::Result<()>
where
    DB: Database,
    for<'a> &'a mut E: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
{
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
    sqlx::query(&format!("INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES ('{id}', '{name}', '{street0}', '{street1}', '{city}', '{state}', '{zip}', '{tax}', '{ytd}')"))
                            .execute(&mut *conn)
                            .await?;
    load_stocks(warehouse, conn, 1000).await?;
    load_districts(warehouse, conn).await?;
    Ok(())
}

async fn load_districts<DB, E>(warehouse: &Warehouse, conn: &mut E) -> anyhow::Result<()>
where
    DB: Database,
    for<'a> &'a mut E: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
{
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
            sqlx::query(&sql[0..sql.len() - 1])
                .execute(&mut *conn)
                .await?;
            sql.clear();
            sql.push_str(SQL);
        }
        load_customers(&district, conn, CUSTOMER_PER_DISTRICT).await?;
        load_orders(&district, conn, ORDERS_PER_DISTRICT / 2).await?;
    }
    Ok(())
}

async fn load_customers<DB, E>(
    district: &District,
    conn: &mut E,
    batch_size: usize,
) -> anyhow::Result<()>
where
    DB: Database,
    for<'a> &'a mut E: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
{
    info!(
        "Loading customers for districts ID={d_id} for warehouse ID={id} (batch size={batch_size})",
        d_id = district.id,
        id = district.warehouse_id,
    );
    assert!(CUSTOMER_PER_DISTRICT % batch_size == 0);
    const CUSTOMER_SQL: &str = "INSERT INTO customer ( c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) VALUES";

    let mut customer_sql = CUSTOMER_SQL.to_string();
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
        if customer.id % (batch_size as u16) == 0 {
            sqlx::query(&customer_sql[0..customer_sql.len() - 1])
                .execute(&mut *conn)
                .await?;
            customer_sql.clear();
            customer_sql.push_str(CUSTOMER_SQL);
        }

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
            sqlx::query(&format!("INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES ('{customer_id}', '{customer_district_id}', '{customer_warehouse_id}', '{district_id}', '{warehouse_id}', NOW(), '{amount}', '{data}')"))
                .execute(&mut *conn)
                .await?;
        }
    }
    Ok(())
}

async fn load_orders<DB, E>(
    district: &District,
    conn: &mut E,
    batch_size: usize,
) -> anyhow::Result<()>
where
    DB: Database,
    for<'a> &'a mut E: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
{
    info!(
        "Loading orders for districts ID={d_id} for warehouse ID={id} (batch size = {batch_size})",
        d_id = district.id,
        id = district.warehouse_id,
    );
    const SQL_ORDER: &str = "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES";
    assert!(ORDERS_PER_DISTRICT % batch_size == 0);
    let mut sql_order = SQL_ORDER.to_string();

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
        sql_order.push_str(&format!("('{id}', '{district_id}', '{warehouse_id}', '{customer_id}', NOW(), {carrier_id}, '{order_lines_count}', '{all_local}'),"));
        if order.id % (batch_size as u32) == 0 {
            sqlx::query(&sql_order[0..sql_order.len() - 1])
                .execute(&mut *conn)
                .await?;
            sql_order.clear();
            sql_order.push_str(SQL_ORDER);
        }

        // TODO batch optimize
        if let Some(new_order) = new_order {
            let NewOrder {
                order_id,
                district_id,
                warehouse_id,
            } = new_order;
            sqlx::query(&format!(
                "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ('{order_id}', '{district_id}', '{warehouse_id}')",
            ))
            .execute(&mut *conn)
            .await?;
        }

        // Insert order lines.
        let mut sql_order_line = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES".to_string();
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
            sql_order_line.push_str(&format!("('{order_id}', '{district_id}', '{warehouse_id}', '{number}', '{item_id}', '{supply_warehouse_id}', NOW(), '{quantity}', '{amount}', '{dist_info}'),"));
        }
        sqlx::query(&sql_order_line[0..sql_order_line.len() - 1])
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

pub async fn load_items<DB, E>(
    generator: ItemGenerator,
    conn: &mut E,
    batch_size: usize,
) -> anyhow::Result<()>
where
    DB: Database,
    for<'a> &'a mut E: sqlx::Executor<'a, Database = DB>,
    for<'a> <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
{
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
            sqlx::query(&sql[0..sql.len() - 1])
                .execute(&mut *conn)
                .await?;
            sql.clear();
            sql.push_str(SQL_PREFIX);
        }
    }
    Ok(())
}
