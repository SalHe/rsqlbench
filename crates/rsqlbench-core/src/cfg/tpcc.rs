use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TpccBenchmark {
    /// Simulating keying and thinking time. Disable this feature for more parallelism with less terminals.
    #[serde(default)]
    pub keying_and_thinking: bool,

    /// Ramp up duration(minutes).
    pub ramp_up: usize,

    /// Benchmark duration(minutes).
    ///
    /// Benchmark will finish after specified minutes.
    pub baking: usize,

    /// Terminals to simulate.
    pub terminals: usize,

    /// TPC-C transactions' weight.
    ///
    /// Unnecessary to specify weight of new order which will be determined by other weights
    /// with assuming that all weights sum to 100.0%.
    pub transactions: TpccTransaction,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TpccTransaction {
    pub payment: f32,
    pub order_status: f32,
    pub delivery: f32,
    pub stock_level: f32,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unsatisfied weight(name, minimal percents): {0:?}")]
    SmallWeight(Vec<(String, f32)>),
}

impl TpccTransaction {
    pub fn verify(&self) -> Result<(), Error> {
        macro_rules! verify_rules {
            ($($weight:expr => $target:expr,)*) => {
                {
                    let mut vec = vec![];
                    $(
                        if *$weight < $target {
                            vec.push((stringify!($weight).to_string(), $target));
                        }
                    )*;
                    if vec.len() == 0{
                        Ok(())
                    }else{
                        Err(Error::SmallWeight(vec))
                    }
                }
            };
        }
        let Self {
            payment,
            order_status,
            delivery,
            stock_level,
        } = self;
        let new_order = self.new_order_weight();
        let new_order = &new_order;
        verify_rules! {
            new_order => 0.0,
            payment => 43.0,
            order_status => 4.0,
            delivery => 4.0,
            stock_level => 4.0,
        }
    }

    pub fn new_order_weight(&self) -> f32 {
        100.0 - self.payment - self.order_status - self.delivery - self.stock_level
    }
}
