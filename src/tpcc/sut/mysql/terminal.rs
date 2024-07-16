use async_trait::async_trait;
use sqlx::MySqlConnection;

use crate::tpcc::{
    sut::{Terminal, TerminalResult},
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
    async fn new_order(&mut self, input: &NewOrder) -> anyhow::Result<TerminalResult> {
        sqlx::query("set @next_order_id = ?")
            .bind(265161)
            .execute(&mut self.conn)
            .await?;
        sqlx::query("CALL NEWORD(?,?,?,?,?, @discount, @lastname, @credit, @district_tax, @warehouse_tax, @next_order_id, NOW())")
            .bind(input.warehouse_id)
            .bind(1)
            .bind(input.district_id)
            .bind(input.customer_id)
            .bind(input.order_lines.len() as u32)
            .execute(&mut self.conn)
            .await?;
        Ok(TerminalResult::Executed(()))
    }

    async fn payment(&mut self, input: &Payment) -> anyhow::Result<()> {
        let (customer_id, name, by_name) = match &input.customer {
            CustomerSelector::LastName(name) => (None, Some(name), true),
            CustomerSelector::ID(id) => (Some(id), None, false),
        };
        sqlx::query("set @customer_id = ?, @last_name = ?")
            .bind(customer_id)
            .bind(name)
            .execute(&mut self.conn)
            .await?;
        sqlx::query("CALL PAYMENT(?, ?, ?, ?, @customer_id, ?, ?, @last_name, @street_1, @street_2, @city, @state, @zip, @d_street_1, @d_street_2, @d_city, @d_state, @d_zip, @first_name, @middle_name, @c_street_1, @c_street_2, @c_city, @c_state, @c_zip, @c_phone, @since, @credit, @credit_lim, @discount, @balance, @data, NOW())")
            .bind(input.warehouse_id)
            .bind(input.district_id)
            .bind(input.warehouse_id)
            .bind(input.district_id)
            .bind(customer_id)
            .bind(by_name)
            .bind(input.amount)
            .bind(name)
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }

    async fn order_status(&mut self, input: &OrderStatus) -> anyhow::Result<()> {
        let (customer_id, name, by_name) = match &input.customer {
            CustomerSelector::LastName(name) => (None, Some(name), true),
            CustomerSelector::ID(id) => (Some(id), None, false),
        };
        sqlx::query("set @customer_id = ?, @last_name = ?")
            .bind(customer_id)
            .bind(name)
            .execute(&mut self.conn)
            .await?;
        sqlx::query("CALL OSTAT(?,?,@customer_id,?,@last_name,@first_name,@middle_name,@balance,@order_id,@entry_date,@carrier_id)")
            .bind(input.warehouse_id)
            .bind(input.district_id)
            .bind(by_name)
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }

    async fn delivery(&mut self, input: &Delivery) -> anyhow::Result<()> {
        sqlx::query("CALL DELIVERY(?,?, NOW())")
            .bind(input.warehouse_id)
            .bind(input.carrier_id)
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }

    async fn stock_level(&mut self, input: &StockLevel) -> anyhow::Result<()> {
        sqlx::query("CALL SLEV(?,?,?, @stock_count)")
            .bind(input.warehouse_id)
            .bind(input.district_id)
            .bind(input.threshold)
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }
}
