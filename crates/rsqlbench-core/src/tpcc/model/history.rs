use time::OffsetDateTime;

use crate::tpcc::random::rand_str;

use super::Customer;

#[derive(Debug)]
pub struct History {
    pub customer_id: u16,
    pub customer_district_id: u8,
    pub customer_warehouse_id: u32,
    pub district_id: u8,
    pub warehouse_id: u32,
    pub date: Option<OffsetDateTime>,
    pub amount: f32,
    pub data: String,
}

pub struct HistoryGenerator {
    customer_id: u16,
    district_id: u8,
    warehouse_id: u32,
    generated: bool,
}

impl HistoryGenerator {
    pub fn from_customer(customer: &Customer) -> Self {
        Self {
            customer_id: customer.id,
            district_id: customer.district_id,
            warehouse_id: customer.warehouse_id,
            generated: false,
        }
    }
}

impl Iterator for HistoryGenerator {
    type Item = History;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.generated {
            self.generated = true;
            Some(History {
                customer_id: self.customer_id,
                customer_district_id: self.district_id,
                customer_warehouse_id: self.warehouse_id,
                district_id: self.district_id,
                warehouse_id: self.warehouse_id,
                date: None,
                amount: 10.00,
                data: rand_str(12, 24),
            })
        } else {
            None
        }
    }
}
