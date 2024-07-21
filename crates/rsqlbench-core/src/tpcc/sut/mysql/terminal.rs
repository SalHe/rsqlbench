use anyhow::Context;
use async_trait::async_trait;
use sqlx::{types::Decimal, MySqlConnection};
use time::OffsetDateTime;

use crate::tpcc::{
    sut::Terminal,
    transaction::{
        CustomerSelector, Delivery, DeliveryOut, NewOrder, NewOrderLineOut, NewOrderOut,
        NewOrderRollbackOut, OrderStatus, OrderStatusOut, Payment, PaymentOut, StockLevel,
        StockLevelOut,
    },
};

pub struct MysqlTerminal {
    conn: MySqlConnection,
    warehouse_count: u32,
}

impl MysqlTerminal {
    pub fn new(conn: MySqlConnection, warehouse_count: u32) -> Self {
        Self {
            conn,
            warehouse_count,
        }
    }
}

#[async_trait]
impl Terminal for MysqlTerminal {
    async fn new_order(
        &mut self,
        input: &NewOrder,
    ) -> anyhow::Result<Result<NewOrderOut, NewOrderRollbackOut>> {
        sqlx::query("set @next_order_id = 10000")
            .execute(&mut self.conn)
            .await?;
        let NewOrder {
            warehouse_id,
            district_id,
            customer_id,
            ..
        } = input;
        // It's complicated to pass generated order lines, just generate in database.
        sqlx::query(&format!("CALL NEWORD('{warehouse_id}','{warehouse_count}','{district_id}','{customer_id}','{orders}', @discount, @lastname, @credit, @district_tax, @warehouse_tax, @next_order_id, NOW())",
                    warehouse_count = self.warehouse_count,
                    orders = input.order_lines.len()
            ))
            .execute(&mut self.conn)
            .await?;

        let tx_result = if !input.rollback_last {
            let (discount, lastname, credit, d_tax, w_tax, order_id): (Decimal, String, String, Decimal, Decimal, i64) =
                sqlx::query_as(
                    "select @discount, @lastname, @credit, @district_tax, @warehouse_tax,@next_order_id",
                )
                .fetch_one(&mut self.conn)
                .await
                .with_context(||"Failed to fetch new order out")?;
            Ok(NewOrderOut {
                warehouse_id: *warehouse_id,
                district_id: *district_id,
                customer_id: *customer_id,
                discount: discount.try_into().unwrap(),
                credit,
                customer_last_name: lastname,
                warehouse_tax: w_tax.try_into().unwrap(),
                district_tax: d_tax.try_into().unwrap(),
                order_id: order_id as u32,
                order_lines: vec![NewOrderLineOut {
                    item_id: 99,
                    warehouse_id: 99,
                    quantity: 99,
                    item_name: "unimplemented for MySQL".to_string(),
                    stock_quantity: 99,
                    brand_generic: "G".to_string(),
                    price: 99.9,
                    amount: 999.9,
                }], // ignore
                entry_date: OffsetDateTime::now_utc(), // just use now
            })
        } else {
            let (lastname, credit, order_id): (String, String, i64) =
                sqlx::query_as("select @lastname, @credit,@next_order_id")
                    .fetch_one(&mut self.conn)
                    .await?;
            Err(NewOrderRollbackOut {
                warehouse_id: *warehouse_id,
                district_id: *district_id,
                customer_id: *customer_id,
                credit,
                customer_last_name: lastname,
                order_id: order_id as u32,
            })
        };
        Ok(tx_result)
    }

    async fn payment(&mut self, input: &Payment) -> anyhow::Result<PaymentOut> {
        let (customer_id, name, by_name) = match &input.customer {
            CustomerSelector::LastName(name) => (None, Some(name), 1),
            CustomerSelector::ID(id) => (Some(id), None, 0),
        };
        sqlx::query(&format!(
            "set @customer_id = {customer_id}, @last_name = '{name}'",
            customer_id = customer_id.copied().unwrap_or_default(),
            name = name.map(|x| x.as_str()).unwrap_or("")
        ))
        .execute(&mut self.conn)
        .await?;
        let Payment {
            warehouse_id,
            district_id,
            amount,
            ..
        } = input;
        sqlx::query(&format!("CALL PAYMENT('{warehouse_id}', '{district_id}', '{warehouse_id}', '{district_id}', @customer_id, '{by_name}', '{amount}', @last_name, @street_1, @street_2, @city, @state, @zip, @d_street_1, @d_street_2, @d_city, @d_state, @d_zip, @first_name, @middle_name, @c_street_1, @c_street_2, @c_city, @c_state, @c_zip, @c_phone, @since, @credit, @credit_lim, @discount, @balance, @data, NOW())"))
            .execute(&mut self.conn)
            .await?;

        let out = {
            let (customer_id,last_name,street_1,street_2,city,state,zip,d_street_1,d_street_2,d_city,d_state,d_zip,first_name,middle_name,c_street_1,c_street_2): (i64, Option<String>, String,String,String, String, String, String, String, String, String, String, Option<String>, Option<String>, Option<String>, Option<String>) = sqlx::query_as("select @customer_id, @last_name, @street_1, @street_2, @city, @state, @zip, @d_street_1, @d_street_2, @d_city, @d_state, @d_zip, @first_name, @middle_name, @c_street_1, @c_street_2")
                .fetch_one(&mut self.conn)
                .await
                .with_context(||"Failed to fetch payment out")?;
            let (c_city,c_state,c_zip,c_phone,_since,credit,credit_lim,discount,balance,data): (Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<Decimal>, Option<Decimal>, Option<Decimal>, Option<String>) = sqlx::query_as("select @c_city, @c_state, @c_zip, @c_phone, @since, @credit, @credit_lim, @discount, @balance, @data")
                .fetch_one(&mut self.conn)
                .await
                .with_context(||"Failed to fetch payment out2")?;
            PaymentOut {
                warehouse_id: *warehouse_id,
                district_id: *district_id,
                customer_id: customer_id as _,
                customer_warehouse_id: input.customer_warehouse_id(),
                customer_district_id: *district_id,
                amount: *amount,
                date: OffsetDateTime::now_utc(),
                warehouse_street: (street_1, street_2),
                warehouse_city: city,
                warehouse_state: state,
                warehouse_zip: zip,
                district_street: (d_street_1, d_street_2),
                district_city: d_city,
                district_state: d_state,
                district_zip: d_zip,
                customer_first_name: first_name,
                customer_last_name: last_name,
                customer_middle_name: middle_name,
                customer_street: (c_street_1, c_street_2),
                customer_city: c_city,
                customer_state: c_state,
                customer_zip: c_zip,
                customer_phone: c_phone,
                customer_since: OffsetDateTime::now_utc(), // TODO parse out
                customer_credit: credit,
                customer_credit_lim: credit_lim.map(|x| x.try_into().unwrap()),
                customer_discount: discount.map(|x| x.try_into().unwrap()),
                customer_balance: balance.map(|x| x.try_into().unwrap()),
                customer_data: data,
            }
        };
        Ok(out)
    }

    async fn order_status(&mut self, input: &OrderStatus) -> anyhow::Result<OrderStatusOut> {
        let (customer_id, name, by_name) = match &input.customer {
            CustomerSelector::LastName(name) => (None, Some(name), 1),
            CustomerSelector::ID(id) => (Some(id), None, 0),
        };
        sqlx::query(&format!(
            "set @customer_id = {customer_id}, @last_name = '{name}'",
            customer_id = customer_id.copied().unwrap_or_default(),
            name = name.map(|x| x.as_str()).unwrap_or("")
        ))
        .execute(&mut self.conn)
        .await?;
        let OrderStatus {
            warehouse_id,
            district_id,
            ..
        } = input;
        sqlx::query(&format!("CALL OSTAT('{warehouse_id}','{district_id}',@customer_id,'{by_name}',@last_name,@first_name,@middle_name,@balance,@order_id,@entry_date,@carrier_id)"))
            .execute(&mut self.conn)
            .await?;
        let (customer_id,last_name, first_name,middle_name,balance, order_id,entry_date, carrier_id) : (Option<i64>,Option<String>, Option<String>,Option<String>,Option<Decimal>,Option<i64>,Option<String>,Option<i64>) = sqlx::query_as("select @customer_id,@last_name,@first_name,@middle_name,@balance,@order_id,@entry_date,@carrier_id").fetch_one(&mut self.conn).await?;
        Ok(OrderStatusOut {
            warehouse_id: *warehouse_id,
            district_id: *district_id,
            customer_id: customer_id.map(|x| x as u32),
            customer_last_name: last_name,
            customer_middle_name: middle_name,
            customer_first_name: first_name,
            customer_balance: balance.map(|x| x.try_into().unwrap()),
            order_id: order_id.map(|x| x as u32),
            carrier_id: carrier_id.map(|x| x as u8),
            entry_date: entry_date.map(|_| OffsetDateTime::now_utc()),
            order_lines: vec![], // unimplemented for MySQL
        })
    }

    async fn delivery(&mut self, input: &Delivery) -> anyhow::Result<DeliveryOut> {
        sqlx::query(&format!(
            "CALL DELIVERY('{}','{}', NOW())",
            input.warehouse_id, input.carrier_id
        ))
        .execute(&mut self.conn)
        .await?;
        Ok(DeliveryOut {
            warehouse_id: input.warehouse_id,
            carrier_id: input.carrier_id,
        })
    }

    async fn stock_level(&mut self, input: &StockLevel) -> anyhow::Result<StockLevelOut> {
        let StockLevel {
            warehouse_id,
            district_id,
            threshold,
        } = input;
        sqlx::query(&format!(
            "CALL SLEV('{warehouse_id}','{district_id}','{threshold}', @stock_count)"
        ))
        .execute(&mut self.conn)
        .await?;
        let (low_stock,): (i64,) = sqlx::query_as("select @stock_count")
            .fetch_one(&mut self.conn)
            .await?;
        Ok(StockLevelOut {
            warehouse_id: *warehouse_id,
            district_id: *district_id,
            threshold: *threshold,
            low_stock: low_stock as _,
        })
    }
}
