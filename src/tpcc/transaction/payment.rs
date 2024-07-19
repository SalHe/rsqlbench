//! Display:
//!
//! ```plain
//!              1         2         3         4         5         6         7         8  
//!     12345678901234567890123456789012345678901234567890123456789012345678901234567890
//!     --------------------------------------------------------------------------------
//!  01|                                     Payment                                    |
//!  02|Date: DD-MM-YYYY hh:mm:ss                                                       |
//!  03|                                                                                |
//!  04|Warehouse: 9999                          District: 99                           |
//!  05|XXXXXXXXXXXXXXXXXXXX                     XXXXXXXXXXXXXXXXXXXX                   |
//!  06|XXXXXXXXXXXXXXXXXXXX                     XXXXXXXXXXXXXXXXXXXX                   |
//!  07|XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX       XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX     |
//!  08|                                                                                |
//!  09|Customer: 9999  Cust-Warehouse: 9999  Cust-District: 99                         |
//!  10|Name:   XXXXXXXXXXXXXXXX XX XXXXXXXXXXXXXXXX     Since:  DD-MM-YYYY             |
//!  11|        XXXXXXXXXXXXXXXXXXXX                     Credit: XX                     |
//!  12|        XXXXXXXXXXXXXXXXXXXX                     %Disc:  99.99                  |
//!  13|        XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX       Phone:  XXXXXX-XXX-XXX-XXXX    |
//!  14|                                                                                |
//!  15|Amount Paid:          $9999.99      New Cust-Balance: $-9999999999.99           |
//!  16|Credit Limit:   $9999999999.99                                                  |
//!  17|                                                                                |
//!  18|Cust-Data: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   |
//!  19|           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   |
//!  20|           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   |
//!  21|           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   |
//!  22|                                                                                |
//!  23|                                                                                |
//!  24|                                                                                |
//!     --------------------------------------------------------------------------------
//! ```
//!

use std::fmt::Display;

use rand::{thread_rng, Rng};
use time::OffsetDateTime;

use crate::tpcc::{
    model::DISTRICT_PER_WAREHOUSE,
    random::{rand_double, rand_last_name, NURAND_CUSTOMER_ID},
};

use super::DATE_FORMAT;

#[derive(Debug)]
pub struct Payment {
    pub warehouse_id: u32,
    pub district_id: u8,
    pub customer: CustomerSelector,
    pub amount: f32,
    preferred_warehouse_id: u32,
}

impl Payment {
    pub fn generate(
        preferred_warehouse_id: u32,
        warehouse_count: u32,
        preferred_district_id: u8,
    ) -> Self {
        let (warehouse_id, district_id) = if thread_rng().gen_bool(0.85) {
            (preferred_warehouse_id, preferred_district_id)
        } else {
            let mut w_id = preferred_warehouse_id;
            if thread_rng().gen_bool(0.01) && warehouse_count > 1 {
                // remote warehouse
                while w_id == preferred_warehouse_id {
                    w_id = thread_rng().gen_range(1..=warehouse_count);
                }
            }
            (
                w_id,
                thread_rng().gen_range(1..=(DISTRICT_PER_WAREHOUSE as u8)),
            )
        };
        Self {
            warehouse_id,
            district_id,
            customer: CustomerSelector::generate(),
            amount: rand_double(1.00, 5000.00, -2) as f32,
            preferred_warehouse_id,
        }
    }

    pub fn is_remote(&self) -> bool {
        self.preferred_warehouse_id == self.warehouse_id
    }
}

impl Display for Payment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let now = OffsetDateTime::now_utc().format(DATE_FORMAT).unwrap();
        let Self {
            warehouse_id: w,
            district_id: d,
            customer,
            amount: a,
            preferred_warehouse_id: pw,
        } = self;
        let (c_, c_last_____) = match customer {
            CustomerSelector::LastName(l) => (String::from("----"), l.as_str()),
            CustomerSelector::ID(c) => (format!("{c:<4}"), ""),
        };
        write!(
            f,
            r#"                                     Payment                                    
Date: {now}                                                       
                                                                                
Warehouse: {w:<6}                        District: {d:<2}                           
XXXXXXXXXXXXXXXXXXXX                     XXXXXXXXXXXXXXXXXXXX                   
XXXXXXXXXXXXXXXXXXXX                     XXXXXXXXXXXXXXXXXXXX                   
XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX       XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX     
                                                                                
Customer: {c_}  Cust-Warehouse: {pw:<4}  Cust-District: {d:<2}                         
Name:   {c_last_____:16} XX XXXXXXXXXXXXXXXX     Since:  **-**-****             
        XXXXXXXXXXXXXXXXXXXX                     Credit: XX                     
        XXXXXXXXXXXXXXXXXXXX                     %Disc:  --.--                  
        XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX       Phone:  XXXXXX-XXX-XXX-XXXX    
                                                                                
Amount Paid:          ${a:4.2}      New Cust-Balance: $-**********.**           
Credit Limit:   $----------.--                                                  
                                                                                
Cust-Data: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   
           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   
           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   
           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   
                                                                                
                                                                                
                                                                                "#
        )?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum CustomerSelector {
    LastName(String),
    ID(u32),
}

impl CustomerSelector {
    pub fn generate() -> Self {
        if thread_rng().gen_bool(0.6) {
            CustomerSelector::LastName(rand_last_name())
        } else {
            CustomerSelector::ID(NURAND_CUSTOMER_ID.next() as _)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::tpcc::transaction::{test::terminal_display, Payment};

    #[test]
    fn display() {
        terminal_display(Payment::generate(29, 30, 29));
    }
}
