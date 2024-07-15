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
