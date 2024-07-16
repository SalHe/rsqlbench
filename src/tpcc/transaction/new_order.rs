use rand::{thread_rng, Rng};

use crate::tpcc::{
    model::{DISTRICT_PER_WAREHOUSE, MAX_ITEMS},
    random::{NURAND_CUSTOMER_ID, NURAND_ITEM_ID},
};

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
        let mut order_lines = (5..=(thread_rng().gen_range(5..=15)))
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

#[derive(Debug)]
pub struct NewOrderLine {
    pub item_id: u32,
    pub warehouse_id: u32,
    pub quantity: u8,
    original_warehouse_id: u32,
}

impl NewOrderLine {
    pub fn is_remote(&self) -> bool {
        self.original_warehouse_id == self.warehouse_id
    }
}
