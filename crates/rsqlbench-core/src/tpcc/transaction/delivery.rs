//! ```plain
//!    1         2         3         4         5         6         7         8  
//!    12345678901234567890123456789012345678901234567890123456789012345678901234567890
//!    --------------------------------------------------------------------------------
//! 01|                                     Delivery                                   |
//! 02|Warehouse: 9999                                                                 |
//! 03|                                                                                |
//! 04|Carrier Number: 99                                                              |
//! 05|                                                                                |
//! 06|Execution Status: XXXXXXXXXXXXXXXXXXXXXXXXX                                     |
//! 07|                                                                                |
//! 08|                                                                                |
//! 09|                                                                                |
//! 10|                                                                                |
//! 11|                                                                                |
//! 12|                                                                                |
//! 13|                                                                                |
//! 14|                                                                                |
//! 15|                                                                                |
//! 16|                                                                                |
//! 17|                                                                                |
//! 18|                                                                                |
//! 19|                                                                                |
//! 20|                                                                                |
//! 21|                                                                                |
//! 22|                                                                                |
//! 23|                                                                                |
//! 24|                                                                                |
//!    --------------------------------------------------------------------------------
//! ```
//!

use std::fmt::Display;

use rand::{thread_rng, Rng};

#[derive(Debug)]
pub struct Delivery {
    pub warehouse_id: u32,
    pub carrier_id: u8,
}

impl Delivery {
    pub fn generate(warehouse_id: u32) -> Self {
        Self {
            warehouse_id,
            carrier_id: thread_rng().gen_range(1..=10),
        }
    }
}

impl Display for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            warehouse_id: w,
            carrier_id: c,
        } = self;
        write!(
            f,
            r#"                                     Delivery                                   
Warehouse: {w:<6}                                                               
                                                                                
Carrier Number: {c:<2}                                                              
                                                                                
Execution Status: -------------------------                                     
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                "#
        )?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct DeliveryOut {
    pub warehouse_id: u32,
    pub carrier_id: u8,
}

impl Display for DeliveryOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            warehouse_id: w,
            carrier_id: c,
        } = self;
        write!(
            f,
            r#"                                     Delivery                                   
Warehouse: {w:<6}                                                               
                                                                                
Carrier Number: {c:<2}                                                              
                                                                                
Execution Status: Delivery has been queued                                      
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                "#
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::tpcc::transaction::test::terminal_display;

    use super::{Delivery, DeliveryOut};

    #[test]
    fn display() {
        terminal_display(Delivery::generate(22));
    }

    #[test]
    fn display_out() {
        terminal_display(DeliveryOut {
            warehouse_id: 1,
            carrier_id: 2,
        });
    }
}
