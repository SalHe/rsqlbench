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

use std::fmt::Display;

use rand::{thread_rng, Rng};

use crate::tpcc::model::DISTRICT_PER_WAREHOUSE;

use super::CustomerSelector;

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

#[cfg(test)]
mod test {
    use crate::tpcc::transaction::test::terminal_display;

    use super::OrderStatus;

    #[test]
    fn display() {
        terminal_display(OrderStatus::generate(22));
    }
}
