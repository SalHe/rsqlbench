//!    1         2         3         4         5         6         7         8  
//!    12345678901234567890123456789012345678901234567890123456789012345678901234567890
//!    --------------------------------------------------------------------------------
//! 01|                                  Stock-Level                                   |
//! 02|Warehouse: 9999   District: 99                                                  |
//! 03|                                                                                |
//! 04|Stock Level Threshold: 99                                                       |
//! 05|                                                                                |
//! 06|low stock: 999                                                                  |
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

use std::fmt::Display;

use rand::{thread_rng, Rng};

#[derive(Debug)]
pub struct StockLevel {
    pub warehouse_id: u32,
    pub district_id: u8,
    pub threshold: u8,
}

impl StockLevel {
    pub fn generate(warehouse_id: u32, district_id: u8) -> Self {
        Self {
            warehouse_id,
            district_id,
            threshold: thread_rng().gen_range(10..=20),
        }
    }
}

impl Display for StockLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            warehouse_id: w,
            district_id: d,
            threshold: t,
        } = self;
        write!(
            f,
            r#"                                  Stock-Level                                   
Warehouse: {w:<6} District: {d:<2}                                                  
                                                                                
Stock Level Threshold: {t:<2}                                                       
                                                                                
low stock: ---                                                                  
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                "#
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::tpcc::transaction::test::terminal_display;

    use super::StockLevel;

    #[test]
    fn display() {
        terminal_display(StockLevel::generate(11, 2));
    }
}
