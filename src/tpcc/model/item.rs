use std::ops::RangeInclusive;

use rand::{thread_rng, Rng};

use crate::tpcc::random::{rand_double, rand_str};

#[derive(Debug)]
pub struct Item {
    pub id: u32,
    pub image_id: u16,
    pub name: String,
    pub price: f32,
    pub data: String,
}

pub struct ItemGenerator {
    range: RangeInclusive<u32>,
}

impl ItemGenerator {
    pub fn new(range: RangeInclusive<u32>) -> Self {
        Self { range }
    }
}

impl Iterator for ItemGenerator {
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|id| Item {
            id,
            image_id: thread_rng().gen_range(1..=10000),
            name: rand_str(14, 24),
            price: rand_double(1.0, 100.0, -2) as _,
            data: rand_str(26, 50),
        })
    }
}
