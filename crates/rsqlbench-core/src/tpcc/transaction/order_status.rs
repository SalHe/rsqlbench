//! ```plain
//!    1         2         3         4         5         6         7         8  
//!    12345678901234567890123456789012345678901234567890123456789012345678901234567890
//!    --------------------------------------------------------------------------------
//! 01|                                  Order-Status                                  |
//! 02|Warehouse: 9999   District: 99                                                  |
//! 03|Customer: 9999   Name: XXXXXXXXXXXXXXXX XX XXXXXXXXXXXXXXXX                     |
//! 04|Cust-Balance: $-99999.99                                                        |
//! 05|                                                                                |
//! 06|Order-Number: 99999999   Entry-Date: DD-MM-YYYY hh:mm:ss   Carrier-Number: 99   |
//! 07|Supply-W     Item-Id    Qty     Amount      Delivery-Date                       |
//! 08|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 09|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 10|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 11|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 12|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 13|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 14|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 15|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 16|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 17|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 18|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 19|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 20|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 21|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 22|  9999       999999     99     $99999.99      DD-MM-YYYY                        |
//! 23|                                                                                |
//! 24|                                                                                |
//!    --------------------------------------------------------------------------------
//! ```

use std::fmt::Display;

use rand::{thread_rng, Rng};
use time::OffsetDateTime;

use crate::tpcc::model::DISTRICT_PER_WAREHOUSE;

use super::{CustomerSelector, SimpleOptionWrapper, DATE_TIME_FORMAT, ONLY_DATE_FORMAT};

#[derive(Debug)]
pub struct OrderStatus {
    pub warehouse_id: u32,
    pub district_id: u8,
    pub customer: CustomerSelector,
}

impl OrderStatus {
    pub fn generate(warehouse_id: u32) -> Self {
        Self {
            warehouse_id,
            district_id: thread_rng().gen_range(1..(DISTRICT_PER_WAREHOUSE as _)),
            customer: CustomerSelector::generate(),
        }
    }
}

impl Display for OrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            warehouse_id: w,
            district_id: d,
            customer,
        } = self;
        let (c_, c_last________) = match customer {
            CustomerSelector::LastName(l) => (String::from("----"), l.as_str()),
            CustomerSelector::ID(c) => (format!("{c:<4}"), ""),
        };
        write!(
            f,
            r#"                                  Order-Status                                  
Warehouse: {w:<6} District: {d:<2}                                                  
Customer: {c_}   Name: {c_last________:<16} XX XXXXXXXXXXXXXXXX                     
Cust-Balance: $-*****.**                                                        
                                                                                
Order-Number: 99999999   Entry-Date: DD-MM-YYYY hh:mm:ss   Carrier-Number: --   
Supply-W     Item-Id    Qty     Amount      Delivery-Date                       
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
  ----       ------     --     $-----.--      DD-MM-YYYY                        
                                                                                
                                                                                "#
        )?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct OrderStatusOut {
    pub warehouse_id: u32,
    pub district_id: u8,
    pub customer_id: Option<u32>,
    pub customer_last_name: Option<String>,
    pub customer_middle_name: Option<String>,
    pub customer_first_name: Option<String>,
    pub customer_balance: Option<f32>,
    pub order_id: Option<u32>,
    pub carrier_id: Option<u8>,
    pub entry_date: Option<OffsetDateTime>,
    pub order_lines: Vec<OrderStatusLineOut>,
}

impl Display for OrderStatusOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            warehouse_id: w,
            district_id: d,
            customer_id: c,
            customer_last_name: cl,
            customer_middle_name: cm,
            customer_first_name: cf,
            customer_balance: cb,
            order_id: o_id,
            entry_date,
            carrier_id,
            order_lines,
        } = self;
        let c = SimpleOptionWrapper(c);
        let cl = SimpleOptionWrapper(cl);
        let cm = SimpleOptionWrapper(cm);
        let cf = SimpleOptionWrapper(cf);
        let cb = SimpleOptionWrapper(cb);
        let o_id = SimpleOptionWrapper(o_id);
        let carrier_id = SimpleOptionWrapper(carrier_id);
        let entry_date = entry_date
            .map(|x| x.format(DATE_TIME_FORMAT).unwrap())
            .unwrap_or_default();
        write!(
            f,
            r#"                                  Order-Status                                  
Warehouse: {w:<6} District: {d:<2}                                                  
Customer: {c:<4}   Name: {cl:<16} {cm:<2} {cf:<16}                     
Cust-Balance: ${cb:<9.2}                                                        
                                                                                
Order-Number: {o_id:<8}   Entry-Date: {entry_date}   Carrier-Number: {carrier_id:<2}   
Supply-W     Item-Id    Qty     Amount      Delivery-Date                       
"#
        )?;
        for i in order_lines {
            writeln!(f, "{i}")?;
        }
        for _ in 0..(15 - order_lines.len()) {
            writeln!(
                f,
                "                                                                                "
            )?;
        }
        write!(
            f,
            r#"                                                                                
                                                                                "#
        )?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct OrderStatusLineOut {
    pub item_id: u32,
    pub warehouse_id: u32,
    pub quantity: u8,
    pub amount: f32,
    pub delivery_date: OffsetDateTime,
}

impl Display for OrderStatusLineOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            item_id: i,
            warehouse_id: w,
            quantity: q,
            amount: a,
            delivery_date: date,
        } = self;
        let date = date.format(ONLY_DATE_FORMAT).unwrap();
        //         Supply-W     Item-Id    Qty     Amount      Delivery-Date
        write!(
            f,
            "  {w:<6}      {i:<6}     {q:<2}     ${a:<7.2}      {date}                        "
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use time::OffsetDateTime;

    use crate::tpcc::transaction::test::terminal_display;

    use super::{OrderStatus, OrderStatusLineOut, OrderStatusOut};

    #[test]
    fn display() {
        terminal_display(OrderStatus::generate(22));
    }

    #[test]
    fn display_out() {
        terminal_display(OrderStatusOut {
            warehouse_id: 1,
            district_id: 2,
            customer_id: Some(3),
            customer_first_name: Some("WAYNE".to_string()),
            customer_last_name: Some("BRUCE".to_string()),
            customer_middle_name: Some("BR".to_string()),
            customer_balance: Some(100.99),
            order_id: Some(12),
            carrier_id: Some(5),
            entry_date: Some(OffsetDateTime::now_utc()),
            order_lines: vec![OrderStatusLineOut {
                item_id: 20,
                warehouse_id: 2,
                quantity: 50,
                amount: 100.0,
                delivery_date: OffsetDateTime::now_utc(),
            }],
        });
    }
}
