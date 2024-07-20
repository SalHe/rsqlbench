//! 4.3.2.1 The term random means independently selected and uniformly distributed over the specified range of
//! values.
//!
//! Comment: For the purpose of populating the initial d atabase only, random numbers can be generated by selecting
//! entries in sequence from a set of at least 10,000 pregenerated random numbers. This technique cannot be used for the
//! field O_OL_CNT.

use std::ops::RangeInclusive;

use once_cell::sync::Lazy;
use rand::{distributions::Alphanumeric, prelude::*};

/// 4.3.2.2 The notation random a-string \[x .. y\] (respectively, n-string \[x .. y\]) represents a string of random
/// alphanumeric (respectively, numeric) characters of a random length of minimum x, maximum y, and mean (y+x)/ 2.
///
/// Comment: The character set used must be able to represent a minimum of 128 different characters. The character set
/// used must include at least 26 lower case letters, 26 upper case letters, and the digits '0' to '9'.
pub fn rand_str(min_len: usize, max_len: usize) -> String {
    let mut rng = thread_rng();
    let n = rng.gen_range(min_len..=max_len);
    rng.sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

/// 4.3.2.3 The customer last name (C_LAST) must be generated by the concatenation of three variable length
/// syllables selected from the following list:
///
/// ```plaintext
///  0    1     2   3    4   5   6     7     8    9
/// BAR OUGHT ABLE PRI PRES ESE ANTI CALLY ATION EING
/// ```
///
/// Given a number between 0 and 999, each of the three syllables is determined by the corresponding digit in the three
/// digit representation of the number. For example, the number 371 generates the name PRICALLYOUGHT, and the
/// number 40 generates the name BARPRESBAR.
pub fn rand_last_name() -> String {
    static TOKENS: [&str; 10] = [
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
    ];
    let index = NURAND_LASTNAME.next(); // index = XYZ
    let mut name = String::with_capacity(15); // possible MAX length
    name.push_str(TOKENS[index / 100]); // X
    name.push_str(TOKENS[(index % 100) / 10]); // Y
    name.push_str(TOKENS[index % 10]); // Z
    name
}

/// 4.3.2.5 The notation random within \[x .. y\] represents a random value independently selected and uniform ly
/// distributed between x and y, inclusively, with a mean of (x+y)/ 2, and with the same number of digits of precision as
/// shown. For example, \[0.01 .. 100.00\] has 10,000 unique values, whereas \[1 ..100\] has only 100 unique va lues.
pub fn rand_double(min: f64, max: f64, precision: isize) -> f64 {
    let mut rng = thread_rng();
    let mut f = rng.gen_range(min..=max);
    let scalar = 10.0f64.powf(-precision as f64);
    f = (scalar * f).round() / scalar;
    f
}

/// 4.3.2.7 The warehouse zip code (W_ZIP), the district zip code (D_ZIP) and the customer zip code (C_ZIP) must be
/// generated by the concatenation of:
///
/// 1. A random n-string of 4 numbers, and
/// 2. The constant string '11111'.
///
/// Given a random n-string between 0 and 9999, the zip codes are determined by concatenating the n -string and the
/// constant '11111'. This will create 10,000 unique zip codes. For example, the n-string 0503 concatenated with 11111,
/// will make the zip code 050311111.
///
/// Comment: With 30,000 customers per warehouse and 10,000 zip codes available, there will be an average of 3
/// customers per warehouse with the same zip code.
pub fn rand_zip() -> String {
    format!("{:04}11111", thread_rng().gen_range(0..=9999))
}

pub struct NURand {
    const_c: usize,
    const_a: usize,
    range: RangeInclusive<usize>,
}

impl NURand {
    pub fn random(&self, x: usize, y: usize) -> usize {
        let mut rng = thread_rng();
        ((rng.gen_range(0..=self.const_a) | (rng.gen_range(x..=y) + self.const_c)) % (y - x + 1))
            + x
    }

    pub fn next(&self) -> usize {
        self.random(*self.range.start(), *self.range.end())
    }
}

pub struct NURandSpawner;

impl NURandSpawner {
    pub fn spawn(&self, const_a: usize, range: RangeInclusive<usize>) -> NURand {
        NURand {
            const_c: thread_rng().gen_range(0..=const_a),
            const_a,
            range,
        }
    }

    pub fn nurand_customer_last(&self) -> NURand {
        self.spawn(255, 0..=999)
    }

    pub fn nurand_customer_id(&self) -> NURand {
        self.spawn(1023, 1..=3000)
    }

    pub fn nuran_item(&self) -> NURand {
        self.spawn(8191, 1..=100000)
    }
}

pub static NURAND_LASTNAME: Lazy<NURand> = Lazy::new(|| NURandSpawner.nurand_customer_last());
pub static NURAND_CUSTOMER_ID: Lazy<NURand> = Lazy::new(|| NURandSpawner.nurand_customer_id());
pub static NURAND_ITEM_ID: Lazy<NURand> = Lazy::new(|| NURandSpawner.nuran_item());