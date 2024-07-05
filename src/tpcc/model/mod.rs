//! 1.3 Define the model in TPC-C test.
mod customer;
mod district;
mod history;
mod item;
mod order;
mod stock;
mod warehouse;

pub use customer::*;
pub use district::*;
pub use history::*;
pub use item::*;
pub use order::*;
pub use stock::*;
pub use warehouse::*;

/// Items to be populated.
pub const MAX_ITEMS: usize = 100_000;

/// Stocks to be populated for one warehouse.
pub const STOCKS_PER_WAREHOUSE: usize = 100_000;

/// Districts to be populated for one warehouse.
pub const DISTRICT_PER_WAREHOUSE: usize = 10;

/// Customers to be populated for one district.
pub const CUSTOMER_PER_DISTRICT: usize = 3000;

/// Orders to be populated for one district.
pub const ORDERS_PER_DISTRICT: usize = 3000;
