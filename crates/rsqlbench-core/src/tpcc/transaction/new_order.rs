//! Terminal Display for new order:
//!
//! ```plain
//!    1         2         3         4         5         6         7         8
//!    12345678901234567890123456789012345678901234567890123456789012345678901234567890
//!    --------------------------------------------------------------------------------
//! 01|                                   New Order                                    |
//! 02|Warehouse: 9999   District: 99                        Date: DD-MM-YYYY hh:mm:ss |
//! 03|Customer:  9999   Name: XXXXXXXXXXXXXXXX   Credit: XX   %Disc: 99.99            |
//! 04|Order Number: 99999999  Number of Lines: 99        W_tax: 99.99   D_tax: 99.99  |
//! 05|                                                                                |
//! 06| Supp_W  Item_Id  Item Name                 Qty  Stock  B/G  Price    Amount    |
//! 07|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 08|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 09|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 10|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 11|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 12|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 13|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 14|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 15|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 16|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 17|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 18|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 19|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 20|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 21|  9999   999999   XXXXXXXXXXXXXXXXXXXXXXXX  99    999    X   $999.99  $9999.99  |
//! 22|Execution Status: XXXXXXXXXXXXXXXXXXXXXXXX                   Total:  $99999.99  |
//! 23|                                                                                |
//! 24|                                                                                |
//!    ---------------------------------------------------------------------------------
//! ```

use std::fmt::Display;

use rand::{thread_rng, Rng};
use time::OffsetDateTime;

use crate::tpcc::{
    model::{DISTRICT_PER_WAREHOUSE, MAX_ITEMS},
    random::{NURAND_CUSTOMER_ID, NURAND_ITEM_ID},
};

use super::DATE_TIME_FORMAT;

#[derive(Debug)]
pub struct NewOrder {
    pub warehouse_id: u32,
    pub district_id: u8,
    pub rollback_last: bool,
    pub customer_id: u32,
    pub order_lines: Vec<NewOrderLine>,
}

impl NewOrder {
    pub fn generate(warehouse_id: u32, warehouse_count: u32) -> NewOrder {
        let rollback_last = thread_rng().gen_bool(0.01);
        let mut order_lines = (1..=(thread_rng().gen_range(5..=15)))
            .map(|_| {
                let mut w_id = warehouse_id;
                if thread_rng().gen_bool(0.01) && warehouse_count > 1 {
                    // remote warehouse
                    while w_id == warehouse_id {
                        w_id = thread_rng().gen_range(1..=warehouse_count);
                    }
                }
                NewOrderLine {
                    item_id: NURAND_ITEM_ID.next() as _,
                    warehouse_id: w_id,
                    quantity: thread_rng().gen_range(1..=10),
                    original_warehouse_id: warehouse_id,
                }
            })
            .collect::<Vec<_>>();
        if rollback_last {
            order_lines.last_mut().unwrap().item_id = MAX_ITEMS as u32 + 1;
        }
        Self {
            warehouse_id,
            district_id: thread_rng().gen_range(1..=(DISTRICT_PER_WAREHOUSE as u8)),
            rollback_last,
            customer_id: NURAND_CUSTOMER_ID.next() as _,
            order_lines,
        }
    }
}

impl Display for NewOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let now = OffsetDateTime::now_utc().format(DATE_TIME_FORMAT).unwrap();
        let Self {
            warehouse_id: w,
            district_id: d,
            customer_id: c,
            order_lines,
            ..
        } = self;
        let nl = order_lines.len();
        #[rustfmt::skip]
        write!(
            f,
r#"                                   New Order                                    
Warehouse: {w:<6} District: {d:<2}                        Date: {now} 
Customer:  {c:<6} Name: ----------------   Credit: --   %Disc: --.--            
Order Number: --------  Number of Lines: {nl:<2}        W_tax: --.--   D_tax: --.--  
                                                                                
 Supp_W  Item_Id  Item Name                 Qty  Stock  B/G  Price    Amount    
"#)?;
        for i in order_lines {
            writeln!(f, "{i}")?;
        }
        for _ in 0..(15 - order_lines.len()) {
            writeln!(
                f,
                "                                                                                "
            )?;
        }
        #[rustfmt::skip]
        write!(
            f,
r#"Execution Status: ------------------------                   Total:  $-----.--  
                                                                                
                                                                                "#
        )?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct NewOrderLine {
    pub item_id: u32,
    pub warehouse_id: u32,
    pub quantity: u8,
    original_warehouse_id: u32,
}

impl Display for NewOrderLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            item_id: i,
            warehouse_id: w,
            quantity: q,
            original_warehouse_id: _,
        } = self;
        //          Supp_W  Item_Id  Item Name                 Qty  Stock  B/G  Price    Amount
        write!(
            f,
            " {w:<6}  {i:<6}   ------------------------  {q:<2}    ---    -   $---.--  $----.--  "
        )?;
        Ok(())
    }
}

impl NewOrderLine {
    pub fn is_remote(&self) -> bool {
        self.original_warehouse_id == self.warehouse_id
    }
}

pub struct NewOrderOut {
    pub warehouse_id: u32,
    pub district_id: u8,
    pub customer_id: u32,
    pub discount: f32,
    pub credit: String,
    pub customer_last_name: String,
    pub warehouse_tax: f32,
    pub district_tax: f32,
    pub order_id: u32,
    pub order_lines: Vec<NewOrderLineOut>,
    pub entry_date: OffsetDateTime,
}

impl Display for NewOrderOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            warehouse_id: w,
            district_id: d,
            customer_id: c,
            discount: dis,
            credit: cr,
            customer_last_name: clname____,
            warehouse_tax: wt,
            district_tax: dt,
            order_id: oi_,
            order_lines,
            entry_date: date,
        } = self;
        let now = date.format(DATE_TIME_FORMAT).unwrap();
        let nl = order_lines.len();
        #[rustfmt::skip]
        write!(
            f,
r#"                                   New Order                                    
Warehouse: {w:<6} District: {d:<2}                        Date: {now} 
Customer:  {c:<6} Name: {clname____:<16}   Credit: {cr}   %Disc: {dis:<5.2}            
Order Number: {oi_:<8}  Number of Lines: {nl:<2}        W_tax: {wt:<5.2}   D_tax: {dt:<5.2}  
                                                                                
 Supp_W  Item_Id  Item Name                 Qty  Stock  B/G  Price    Amount    
"#)?;
        for i in order_lines {
            writeln!(f, "{i}")?;
        }
        let total: f32 = order_lines.iter().map(|x| x.amount).sum();
        for _ in 0..(15 - order_lines.len()) {
            writeln!(
                f,
                "                                                                                "
            )?;
        }
        #[rustfmt::skip]
        write!(
            f,
r#"Execution Status:                                            Total:  ${total:<8.2}  
                                                                                
                                                                                "#
        )?;
        Ok(())
    }
}

pub struct NewOrderLineOut {
    pub item_id: u32,
    pub warehouse_id: u32,
    pub quantity: u8,
    pub item_name: String,
    pub stock_quantity: u16,
    pub brand_generic: String,
    pub price: f32,
    pub amount: f32,
}

impl Display for NewOrderLineOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            item_id: i,
            warehouse_id: w,
            quantity: q,
            item_name: iname_____________,
            stock_quantity: sq,
            brand_generic: bg,
            price: pr,
            amount: a,
        } = self;
        //          Supp_W  Item_Id  Item Name                 Qty  Stock  B/G  Price    Amount
        write!(f, " {w:<6}  {i:<6}   {iname_____________:<24}  {q:<2}    {sq:<3}    {bg}   ${pr:<6.2}  ${a:<7.2}  ")?;
        Ok(())
    }
}

pub struct NewOrderRollbackOut {
    pub warehouse_id: u32,
    pub district_id: u8,
    pub customer_id: u32,
    pub credit: String,
    pub customer_last_name: String,
    pub order_id: u32,
}

impl Display for NewOrderRollbackOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            warehouse_id: w,
            district_id: d,
            customer_id: c,
            credit: cr,
            customer_last_name: clname____,
            order_id: oi_,
        } = self;
        #[rustfmt::skip]
        write!(
            f,
r#"                                   New Order                                    
Warehouse: {w:<6} District: {d:<2}                        Date:                     
Customer:  {c:<6} Name: {clname____:<16}   Credit: {cr}   %Disc:                  
Order Number: {oi_:<8}  Number of Lines:           W_tax:         D_tax:        
                                                                                
 Supp_W  Item_Id  Item Name                 Qty  Stock  B/G  Price    Amount    
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
Execution Status: Item number is not valid                   Total:  $-----.--  
                                                                                
                                                                                "#)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use time::OffsetDateTime;

    use crate::tpcc::transaction::test::terminal_display;

    use super::{NewOrder, NewOrderLineOut, NewOrderOut, NewOrderRollbackOut};

    #[test]
    fn display() {
        terminal_display(NewOrder::generate(1, 2));
    }

    #[test]
    fn display_executed() {
        terminal_display(NewOrderOut {
            warehouse_id: 1,
            district_id: 2,
            customer_id: 3,
            discount: 5.5,
            credit: "GC".to_string(),
            customer_last_name: "SALHE".to_string(),
            warehouse_tax: 3.2,
            district_tax: 2.5,
            order_id: 45,
            order_lines: vec![
                NewOrderLineOut {
                    item_id: 1,
                    warehouse_id: 1,
                    quantity: 5,
                    item_name: "APPLE".to_string(),
                    stock_quantity: 20,
                    brand_generic: "B".to_string(),
                    price: 2.0,
                    amount: 10.0,
                },
                NewOrderLineOut {
                    item_id: 1,
                    warehouse_id: 1,
                    quantity: 5,
                    item_name: "APPLE".to_string(),
                    stock_quantity: 20,
                    brand_generic: "B".to_string(),
                    price: 2.0,
                    amount: 10.0,
                },
            ],
            entry_date: OffsetDateTime::now_utc(),
        });
    }

    #[test]
    fn display_rollback() {
        terminal_display(NewOrderRollbackOut {
            warehouse_id: 1,
            district_id: 2,
            customer_id: 3,
            credit: "GC".to_string(),
            customer_last_name: "SALHE".to_string(),
            order_id: 45,
        });
    }
}
