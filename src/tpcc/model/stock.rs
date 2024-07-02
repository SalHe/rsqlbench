use std::ops::RangeInclusive;

use rand::{thread_rng, Rng};

use crate::tpcc::random::rand_str;

use super::Warehouse;

#[derive(Debug)]
pub struct Stock {
    pub item_id: u32,
    pub warehouse_id: u32,
    pub quantity: u16,
    pub dist: [String; 10],
    pub ytd: u32,
    pub order_count: u16,
    pub remote_count: u16,
    pub data: String,
}

pub struct StockGenerator {
    warehouse_id: u32,
    range: RangeInclusive<u32>,
}

impl StockGenerator {
    pub fn from_warehouse(warehouse: &Warehouse) -> Self {
        Self {
            warehouse_id: warehouse.id,
            range: 1..=100000,
        }
    }
}

fn rand_data() -> String {
    let mut data = rand_str(26, 50);
    if thread_rng().gen_ratio(1, 10) {
        static ORIGINAL: &str = "ORIGINAL";
        let pos = thread_rng().gen_range(0..(data.len() - ORIGINAL.len()));
        data.replace_range(pos..(pos + ORIGINAL.len()), ORIGINAL);
    }
    data
}

impl Iterator for StockGenerator {
    type Item = Stock;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|item_id| Stock {
            item_id,
            warehouse_id: self.warehouse_id,
            quantity: thread_rng().gen_range(10..=100),
            dist: [
                rand_str(24, 24),
                rand_str(24, 24),
                rand_str(24, 24),
                rand_str(24, 24),
                rand_str(24, 24),
                rand_str(24, 24),
                rand_str(24, 24),
                rand_str(24, 24),
                rand_str(24, 24),
                rand_str(24, 24),
            ],
            ytd: 0,
            order_count: 0,
            remote_count: 0,
            data: rand_data(),
        })
    }
}
