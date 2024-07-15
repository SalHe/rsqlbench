use rand::{thread_rng, Rng};

use crate::tpcc::{
    model::{CUSTOMER_PER_DISTRICT, DISTRICT_PER_WAREHOUSE},
    random::{rand_double, rand_last_name},
};

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
            CustomerSelector::ID(thread_rng().gen_range(1..=CUSTOMER_PER_DISTRICT) as _)
        }
    }
}
