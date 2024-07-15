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
