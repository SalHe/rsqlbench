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
