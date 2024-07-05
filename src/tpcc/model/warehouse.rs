use std::ops::RangeInclusive;

use crate::tpcc::random::{rand_double, rand_str, rand_zip};

#[derive(Debug, Clone)]
pub struct Warehouse {
    pub id: u32,
    pub name: String,
    pub street: (String, String),
    pub city: String,
    pub state: String,
    pub zip: String,
    pub tax: f32,
    pub ytd: f64,
}

pub struct WarehouseGenerator {
    range: RangeInclusive<u32>,
}

impl WarehouseGenerator {
    pub fn new(range: RangeInclusive<u32>) -> Self {
        Self { range }
    }
}

impl Iterator for WarehouseGenerator {
    type Item = Warehouse;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|id| Warehouse {
            id,
            name: rand_str(4, 10),
            street: (rand_str(10, 20), rand_str(10, 20)),
            city: rand_str(10, 20),
            state: rand_str(2, 2),
            zip: rand_zip(),
            tax: rand_double(0.0, 0.2, -1) as _,
            ytd: 300000.0,
        })
    }
}
