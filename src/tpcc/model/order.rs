use std::{iter::Zip, ops::RangeInclusive};

use rand::{prelude::*, thread_rng, Rng};
use time::OffsetDateTime;

use crate::tpcc::random::{rand_double, rand_str};

use super::{District, ORDERS_PER_DISTRICT};

#[derive(Debug)]
pub struct NewOrder {
    pub order_id: u32,
    pub district_id: u8,
    pub warehouse_id: u32,
}

#[derive(Debug)]
pub struct OrderLine {
    pub order_id: u32,
    pub district_id: u8,
    pub warehouse_id: u32,
    pub number: u8,
    pub item_id: u32,
    pub supply_warehouse_id: u32,
    pub delivery_date: Option<OffsetDateTime>,
    pub quantity: u8,
    pub amount: f32,
    pub dist_info: String,
}

#[derive(Debug)]
pub struct Order {
    pub id: u32,
    pub district_id: u8,
    pub warehouse_id: u32,
    pub customer_id: u16,
    pub entry_date: Option<OffsetDateTime>,
    pub carrier_id: Option<u8>,
    pub order_lines_count: u8,
    pub all_local: bool,
}

pub struct OrderGenerator {
    district_id: u8,
    warehouse_id: u32,
    id_range: Zip<RangeInclusive<u32>, std::vec::IntoIter<u16>>,
}

impl OrderGenerator {
    pub fn from_district(district: &District) -> Self {
        let mut rng = thread_rng();
        let mut customer_id = (1..=(ORDERS_PER_DISTRICT as _)).collect::<Vec<u16>>();
        customer_id.shuffle(&mut rng);
        Self {
            district_id: district.id,
            warehouse_id: district.warehouse_id,
            id_range: (1..=(ORDERS_PER_DISTRICT as _)).zip(customer_id),
        }
    }
}

impl Iterator for OrderGenerator {
    type Item = (Order, Option<NewOrder>);

    fn next(&mut self) -> Option<Self::Item> {
        self.id_range.next().map(|(id, customer_id)| {
            let order = Order {
                id,
                district_id: self.district_id,
                warehouse_id: self.warehouse_id,
                customer_id,
                entry_date: None,
                carrier_id: if id < 2101 {
                    Some(thread_rng().gen_range(1..=10))
                } else {
                    None
                },
                order_lines_count: thread_rng().gen_range(5..=15),
                all_local: true,
            };
            let new_order = if id <= 2100 {
                None
            } else {
                Some(NewOrder {
                    order_id: id,
                    district_id: self.district_id,
                    warehouse_id: self.warehouse_id,
                })
            };
            (order, new_order)
        })
    }
}

pub struct OrderLineGenerator {
    order_id: u32,
    district_id: u8,
    warehouse_id: u32,
    entry_date: Option<OffsetDateTime>,
    id: RangeInclusive<u8>,
}

impl OrderLineGenerator {
    pub fn from_order(order: &Order) -> Self {
        Self {
            order_id: order.id,
            district_id: order.district_id,
            warehouse_id: order.warehouse_id,
            entry_date: order.entry_date,
            id: 1..=order.order_lines_count,
        }
    }
}

impl Iterator for OrderLineGenerator {
    type Item = OrderLine;

    fn next(&mut self) -> Option<Self::Item> {
        self.id.next().map(|id| OrderLine {
            order_id: self.order_id,
            district_id: self.district_id,
            warehouse_id: self.warehouse_id,
            number: id,
            item_id: thread_rng().gen_range(1..=100000),
            supply_warehouse_id: self.warehouse_id,
            delivery_date: if self.order_id < 2101 {
                self.entry_date
            } else {
                None
            },
            quantity: 5,
            amount: if self.order_id < 2101 {
                0.0
            } else {
                rand_double(0.01, 9999.99, -2) as _
            },
            dist_info: rand_str(24, 24),
        })
    }
}
