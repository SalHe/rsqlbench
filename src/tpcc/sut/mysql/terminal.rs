use async_trait::async_trait;
use sqlx::MySqlConnection;

use crate::tpcc::{
    sut::Terminal,
    transaction::{CustomerSelector, Delivery, NewOrder, OrderStatus, Payment, StockLevel},
};

pub struct MysqlTerminal {
    conn: MySqlConnection,
}

impl MysqlTerminal {
    pub fn new(conn: MySqlConnection) -> Self {
        Self { conn }
    }
}

// TODO reimplement stored procs
#[async_trait]
impl Terminal for MysqlTerminal {
    async fn new_order(&mut self, input: &NewOrder) -> anyhow::Result<()> {
        sqlx::query("set @next_order_id = 10000")
            .execute(&mut self.conn)
            .await?;
        let NewOrder {
            warehouse_id,
            district_id,
            customer_id,
            ..
        } = input;
        sqlx::query(&format!("CALL NEWORD('{warehouse_id}','{warehouse_count}','{district_id}','{customer_id}','{orders}', @discount, @lastname, @credit, @district_tax, @warehouse_tax, @next_order_id, NOW())",
                    warehouse_count = 1,
                    orders = input.order_lines.len()
            ))
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }

    async fn payment(&mut self, input: &Payment) -> anyhow::Result<()> {
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
        Ok(())
    }

    async fn order_status(&mut self, input: &OrderStatus) -> anyhow::Result<()> {
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
        Ok(())
    }

    async fn delivery(&mut self, input: &Delivery) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "CALL DELIVERY('{}','{}', NOW())",
            input.warehouse_id, input.carrier_id
        ))
        .execute(&mut self.conn)
        .await?;
        Ok(())
    }

    async fn stock_level(&mut self, input: &StockLevel) -> anyhow::Result<()> {
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
        Ok(())
    }
}
