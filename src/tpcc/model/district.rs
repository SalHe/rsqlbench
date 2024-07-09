use std::ops::RangeInclusive;

use crate::tpcc::random::{rand_double, rand_str, rand_zip};

use super::{Warehouse, DISTRICT_PER_WAREHOUSE};

#[derive(Debug)]
pub struct District {
    pub id: u8,
    pub warehouse_id: u32,
    pub name: String,
    pub street: (String, String),
    pub city: String,
    pub state: String,
    pub zip: String,
    pub tax: f32,
    pub ytd: f64,
    pub next_order_id: u32,
}

pub struct DistrictGenerator {
    id_range: RangeInclusive<u8>,
    warehouse_id: u32,
}

impl DistrictGenerator {
    pub fn from_warehouse(warehouse: &Warehouse) -> Self {
        Self {
            id_range: 1..=(DISTRICT_PER_WAREHOUSE as _),
            warehouse_id: warehouse.id,
        }
    }
}

impl Iterator for DistrictGenerator {
    type Item = District;

    fn next(&mut self) -> Option<Self::Item> {
        self.id_range.next().map(|id| District {
            id,
            warehouse_id: self.warehouse_id,
            name: rand_str(6, 10),
            street: (rand_str(10, 20), rand_str(10, 20)),
            city: rand_str(10, 20),
            state: rand_str(2, 2),
            zip: rand_zip(),
            tax: rand_double(0.0, 0.2, 0) as _,
            ytd: 30000.00,
            next_order_id: 3001,
        })
    }
}
