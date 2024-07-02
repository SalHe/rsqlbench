use rsqlbench::tpcc::{
    model::{ItemGenerator, WarehouseGenerator},
    sut::{FakeSut, Sut},
};
use tracing::info;

fn main() {
    tracing_subscriber::fmt::init();

    info!("Hello tpcc");

    FakeSut.build_schema();
    let loader = FakeSut.loader();
    loader.load_items(ItemGenerator::new(1..=3000));
    loader.load_warehouses(WarehouseGenerator::new(1..=10));
    FakeSut.destroy_schema();
}
