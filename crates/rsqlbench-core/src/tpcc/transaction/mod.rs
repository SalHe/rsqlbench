mod delivery;
mod new_order;
mod order_status;
mod payment;
mod stock_level;

pub use delivery::*;
pub use new_order::*;
pub use order_status::*;
pub use payment::*;
use rand::{thread_rng, Rng};
pub use stock_level::*;

use std::{fmt::Display, time::Duration};

use crate::cfg::tpcc::TpccTransaction;

pub const DATE_TIME_FORMAT: &[time::format_description::FormatItem] =
    time::macros::format_description!("[day]-[month]-[year] [hour]:[minute]:[second]");
pub const ONLY_DATE_FORMAT: &[time::format_description::FormatItem] =
    time::macros::format_description!("[day]-[month]-[year]");

#[derive(Debug)]
pub enum Transaction {
    NewOrder(NewOrder),
    Payment(Payment),
    OrderStatus(OrderStatus),
    Delivery(Delivery),
    StockLevel(StockLevel),
}

impl Transaction {
    pub fn generate(
        tx_weights: &TpccTransaction,
        warehouse_id: u32,
        district_id: u8,
        warehouse_count: u32,
    ) -> Self {
        let picker = thread_rng().gen_range(0.0..=100.0);
        if picker < tx_weights.payment {
            Transaction::Payment(Payment::generate(
                warehouse_id,
                warehouse_count,
                district_id,
            ))
        } else if picker < tx_weights.payment + tx_weights.order_status {
            Transaction::OrderStatus(OrderStatus::generate(warehouse_id))
        } else if picker < tx_weights.payment + tx_weights.order_status + tx_weights.delivery {
            Transaction::Delivery(Delivery::generate(warehouse_id))
        } else if picker
            < tx_weights.payment
                + tx_weights.order_status
                + tx_weights.delivery
                + tx_weights.stock_level
        {
            Transaction::StockLevel(StockLevel::generate(warehouse_id, district_id))
        } else {
            Transaction::NewOrder(NewOrder::generate(warehouse_id, warehouse_count))
        }
    }

    pub fn keying_duration(&self) -> Duration {
        match self {
            Transaction::NewOrder(_) => Duration::from_secs(18),
            Transaction::Payment(_) => Duration::from_secs(3),
            Transaction::OrderStatus(_) => Duration::from_secs(2),
            Transaction::Delivery(_) => Duration::from_secs(2),
            Transaction::StockLevel(_) => Duration::from_secs(2),
        }
    }

    pub fn thinking_duration(&self) -> Duration {
        match self {
            Transaction::NewOrder(_) => Duration::from_secs(12),
            Transaction::Payment(_) => Duration::from_secs(12),
            Transaction::OrderStatus(_) => Duration::from_secs(10),
            Transaction::Delivery(_) => Duration::from_secs(5),
            Transaction::StockLevel(_) => Duration::from_secs(5),
        }
    }
}

pub(crate) struct ZipWrapper<'a>(&'a str);

impl<'a> Display for ZipWrapper<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", &self.0[0..5], &self.0[5..])
    }
}

pub(crate) struct PhoneWrapper<'a>(&'a str);

impl<'a> Display for PhoneWrapper<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}-{}",
            &self.0[0..6],
            &self.0[6..9],
            &self.0[9..12],
            &self.0[12..]
        )
    }
}

pub(crate) struct OptionWrapper<'a, T: ?Sized, WF>(Option<&'a T>, WF);

impl<'a, T: ?Sized, WF, W> Display for OptionWrapper<'a, T, WF>
where
    WF: Fn(&'a T) -> W,
    W: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(v) => self.1(v).fmt(f),
            None => "".fmt(f),
        }
    }
}

pub(crate) struct SimpleOptionWrapper<'a, T>(&'a Option<T>);

impl<'a, T> Display for SimpleOptionWrapper<'a, T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(v) => v.fmt(f),
            None => "".fmt(f),
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::fmt::Display;

    use crate::tpcc::sut::{TERMINAL_HEIGHT, TERMINAL_WIDTH};

    pub fn terminal_display(d: impl Display) {
        let output = format!("{d}");
        let mut to_display = String::with_capacity((TERMINAL_WIDTH + 6) * (TERMINAL_HEIGHT + 5));
        to_display.push_str(
            r#"
              1         2         3         4         5         6         7         8
     12345678901234567890123456789012345678901234567890123456789012345678901234567890
     --------------------------------------------------------------------------------
"#,
        );
        let lines = output.split('\n').collect::<Vec<_>>();
        for (line_no, line) in lines.iter().enumerate() {
            to_display.push_str(&format!(
                "  {l:02}|{line}|{problem}\n",
                l = line_no + 1,
                problem = if line_no >= TERMINAL_HEIGHT {
                    "  <-------- exceeded line"
                } else if line.len() == TERMINAL_WIDTH {
                    ""
                } else {
                    "  <-------- line width not match"
                }
            ));
        }
        to_display.push_str(
            "     --------------------------------------------------------------------------------",
        );

        // println!("{to_display}");
        // strict
        assert!(
            lines.len() == TERMINAL_HEIGHT && lines.iter().all(|x| x.len() == TERMINAL_WIDTH),
            "{to_display}",
        );
    }
}
