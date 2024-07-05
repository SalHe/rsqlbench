use std::ops::RangeInclusive;

use rand::{thread_rng, Rng};
use time::OffsetDateTime;

use crate::tpcc::random::{rand_double, rand_last_name, rand_str, rand_zip};

use super::{District, CUSTOMER_PER_DISTRICT};

#[derive(Debug)]
pub struct Customer {
    pub id: u16,
    pub district_id: u8,
    pub warehouse_id: u32,
    pub first_name: String,
    pub middle_name: String,
    pub last_name: String,
    pub street: (String, String),
    pub city: String,
    pub state: String,
    pub zip: String,
    pub phone: String,
    pub since: Option<OffsetDateTime>,
    pub credit: String,
    pub credit_limit: f64,
    pub discount: f32,
    pub balance: f64,
    pub ytd_payment: f64,
    pub payment_count: u16,
    pub delivery_count: u16,
    pub data: String,
}

pub struct CustomerGenerator {
    district_id: u8,
    warehouse_id: u32,
    id_range: RangeInclusive<u16>,
}

impl CustomerGenerator {
    pub fn from_district(district: &District) -> Self {
        Self {
            district_id: district.id,
            warehouse_id: district.warehouse_id,
            id_range: 1..=(CUSTOMER_PER_DISTRICT as _),
        }
    }
}

impl Iterator for CustomerGenerator {
    type Item = Customer;

    fn next(&mut self) -> Option<Self::Item> {
        self.id_range.next().map(|id| Customer {
            id,
            district_id: self.district_id,
            warehouse_id: self.warehouse_id,
            first_name: rand_str(8, 16),
            middle_name: "OE".to_string(),
            last_name: rand_last_name(),
            street: (rand_str(10, 20), rand_str(10, 20)),
            city: rand_str(10, 20),
            state: rand_str(2, 2),
            zip: rand_zip(),
            phone: format!(
                "{:08}{:08}",
                thread_rng().gen_range(0..=9999_9999),
                thread_rng().gen_range(0..=9999_9999)
            ),
            since: None,
            credit: if thread_rng().gen_ratio(1, 10) {
                "GC"
            } else {
                "BC"
            }
            .to_string(),
            credit_limit: 50000.0,
            discount: rand_double(0.0, 0.5, -4) as _,
            balance: -10.0,
            ytd_payment: 10.0,
            payment_count: 1,
            delivery_count: 0,
            data: rand_str(300, 500),
        })
    }
}
