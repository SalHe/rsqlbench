mod delivery;
mod new_order;
mod order_status;
mod payment;
mod stock_level;

pub use delivery::*;
pub use new_order::*;
pub use order_status::*;
pub use payment::*;
pub use stock_level::*;

use std::time::Duration;

#[derive(Debug)]
pub enum Transaction {
    NewOrder(NewOrder),
    Payment(Payment),
    OrderStatus(OrderStatus),
    Delivery(Delivery),
    StockLevel(StockLevel),
}

impl Transaction {
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
