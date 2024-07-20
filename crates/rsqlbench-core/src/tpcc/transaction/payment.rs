//! Display:
//!
//! ```plain
//!              1         2         3         4         5         6         7         8  
//!     12345678901234567890123456789012345678901234567890123456789012345678901234567890
//!     --------------------------------------------------------------------------------
//!  01|                                     Payment                                    |
//!  02|Date: DD-MM-YYYY hh:mm:ss                                                       |
//!  03|                                                                                |
//!  04|Warehouse: 9999                          District: 99                           |
//!  05|XXXXXXXXXXXXXXXXXXXX                     XXXXXXXXXXXXXXXXXXXX                   |
//!  06|XXXXXXXXXXXXXXXXXXXX                     XXXXXXXXXXXXXXXXXXXX                   |
//!  07|XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX       XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX     |
//!  08|                                                                                |
//!  09|Customer: 9999  Cust-Warehouse: 9999  Cust-District: 99                         |
//!  10|Name:   XXXXXXXXXXXXXXXX XX XXXXXXXXXXXXXXXX     Since:  DD-MM-YYYY             |
//!  11|        XXXXXXXXXXXXXXXXXXXX                     Credit: XX                     |
//!  12|        XXXXXXXXXXXXXXXXXXXX                     %Disc:  99.99                  |
//!  13|        XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX       Phone:  XXXXXX-XXX-XXX-XXXX    |
//!  14|                                                                                |
//!  15|Amount Paid:          $9999.99      New Cust-Balance: $-9999999999.99           |
//!  16|Credit Limit:   $9999999999.99                                                  |
//!  17|                                                                                |
//!  18|Cust-Data: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   |
//!  19|           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   |
//!  20|           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   |
//!  21|           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   |
//!  22|                                                                                |
//!  23|                                                                                |
//!  24|                                                                                |
//!     --------------------------------------------------------------------------------
//! ```
//!

use std::fmt::Display;

use rand::{thread_rng, Rng};
use time::OffsetDateTime;

use crate::tpcc::{
    model::DISTRICT_PER_WAREHOUSE,
    random::{rand_double, rand_last_name, NURAND_CUSTOMER_ID},
};

use super::{
    OptionWrapper, PhoneWrapper, SimpleOptionWrapper, ZipWrapper, DATE_TIME_FORMAT,
    ONLY_DATE_FORMAT,
};

#[derive(Debug)]
pub struct Payment {
    pub warehouse_id: u32,
    pub district_id: u8,
    pub customer: CustomerSelector,
    pub amount: f32,
    preferred_warehouse_id: u32,
}

impl Payment {
    pub fn generate(
        preferred_warehouse_id: u32,
        warehouse_count: u32,
        preferred_district_id: u8,
    ) -> Self {
        let (warehouse_id, district_id) = if thread_rng().gen_bool(0.85) {
            (preferred_warehouse_id, preferred_district_id)
        } else {
            let mut w_id = preferred_warehouse_id;
            if thread_rng().gen_bool(0.01) && warehouse_count > 1 {
                // remote warehouse
                while w_id == preferred_warehouse_id {
                    w_id = thread_rng().gen_range(1..=warehouse_count);
                }
            }
            (
                w_id,
                thread_rng().gen_range(1..=(DISTRICT_PER_WAREHOUSE as u8)),
            )
        };
        Self {
            warehouse_id,
            district_id,
            customer: CustomerSelector::generate(),
            amount: rand_double(1.00, 5000.00, -2) as f32,
            preferred_warehouse_id,
        }
    }

    pub fn customer_warehouse_id(&self) -> u32 {
        self.preferred_warehouse_id
    }

    pub fn is_remote(&self) -> bool {
        self.preferred_warehouse_id == self.warehouse_id
    }
}

impl Display for Payment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let now = OffsetDateTime::now_utc().format(DATE_TIME_FORMAT).unwrap();
        let Self {
            warehouse_id: w,
            district_id: d,
            customer,
            amount: a,
            preferred_warehouse_id: pw,
        } = self;
        let (c_, c_last_____) = match customer {
            CustomerSelector::LastName(l) => (String::from("----"), l.as_str()),
            CustomerSelector::ID(c) => (format!("{c:<4}"), ""),
        };
        write!(
            f,
            r#"                                     Payment                                    
Date: {now}                                                       
                                                                                
Warehouse: {w:<6}                        District: {d:<2}                           
XXXXXXXXXXXXXXXXXXXX                     XXXXXXXXXXXXXXXXXXXX                   
XXXXXXXXXXXXXXXXXXXX                     XXXXXXXXXXXXXXXXXXXX                   
XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX       XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX     
                                                                                
Customer: {c_}  Cust-Warehouse: {pw:<4}  Cust-District: {d:<2}                         
Name:   {c_last_____:16} XX XXXXXXXXXXXXXXXX     Since:  **-**-****             
        XXXXXXXXXXXXXXXXXXXX                     Credit: XX                     
        XXXXXXXXXXXXXXXXXXXX                     %Disc:  --.--                  
        XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX       Phone:  XXXXXX-XXX-XXX-XXXX    
                                                                                
Amount Paid:          ${a:7.2}      New Cust-Balance: $-**********.**           
Credit Limit:   $----------.--                                                  
                                                                                
Cust-Data: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   
           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   
           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   
           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX                   
                                                                                
                                                                                
                                                                                "#
        )?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum CustomerSelector {
    LastName(String),
    ID(u32),
}

impl CustomerSelector {
    pub fn generate() -> Self {
        if thread_rng().gen_bool(0.6) {
            CustomerSelector::LastName(rand_last_name())
        } else {
            CustomerSelector::ID(NURAND_CUSTOMER_ID.next() as _)
        }
    }
}

#[derive(Debug)]
pub struct PaymentOut {
    pub warehouse_id: u32,
    pub district_id: u8,
    pub customer_id: u32,
    pub customer_warehouse_id: u32,
    pub customer_district_id: u8,
    pub amount: f32,
    pub date: OffsetDateTime,
    pub warehouse_street: (String, String),
    pub warehouse_city: String,
    pub warehouse_state: String,
    pub warehouse_zip: String,
    pub district_street: (String, String),
    pub district_city: String,
    pub district_state: String,
    pub district_zip: String,
    pub customer_first_name: Option<String>,
    pub customer_last_name: Option<String>,
    pub customer_middle_name: Option<String>,
    pub customer_street: (Option<String>, Option<String>),
    pub customer_city: Option<String>,
    pub customer_state: Option<String>,
    pub customer_zip: Option<String>,
    pub customer_phone: Option<String>,
    pub customer_since: OffsetDateTime,
    pub customer_credit: Option<String>,
    pub customer_credit_lim: Option<f32>,
    pub customer_discount: Option<f32>,
    pub customer_balance: Option<f32>,
    pub customer_data: Option<String>,
}

impl Display for PaymentOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            warehouse_id: w,
            district_id: d,
            amount: a,
            customer_id,
            customer_warehouse_id: cw,
            customer_district_id: cd,
            date,
            warehouse_street: (w_street1_____, w_street2_____),
            warehouse_city,
            warehouse_state,
            warehouse_zip: w_zip,
            district_street: (d_street1_____, d_street2_____),
            district_city,
            district_state,
            district_zip: d_zip,
            customer_first_name,
            customer_last_name: c_last_____,
            customer_middle_name,
            customer_street: (c_street1_____, c_street2_____),
            customer_city: customer_city_,
            customer_state,
            customer_zip: c_zip,
            customer_phone,
            customer_since: c_since,
            customer_credit,
            customer_credit_lim: c_cre_l,
            customer_discount,
            customer_balance,
            customer_data,
        } = self;
        let now = date.format(DATE_TIME_FORMAT).unwrap();
        let w_zip = ZipWrapper(w_zip);
        let d_zip = ZipWrapper(d_zip);
        let c_zip = OptionWrapper(c_zip.as_ref().map(|x| x.as_str()), ZipWrapper);
        let c_phone = OptionWrapper(customer_phone.as_ref().map(|x| x.as_str()), PhoneWrapper);
        let c_since = c_since.format(ONLY_DATE_FORMAT).unwrap();
        let (c_data1, c_data2, c_data3, c_data4) = match customer_data {
            Some(data) => (
                &data[0..50],
                &data[50..100],
                &data[100..150],
                &data[150..200],
            ),
            None => ("", "", "", ""),
        };

        let c_last_____ = SimpleOptionWrapper(c_last_____);
        let customer_middle_name = SimpleOptionWrapper(customer_middle_name);
        let customer_first_name = SimpleOptionWrapper(customer_first_name);
        let c_street1_____ = SimpleOptionWrapper(c_street1_____);
        let c_street2_____ = SimpleOptionWrapper(c_street2_____);
        let customer_city_ = SimpleOptionWrapper(customer_city_);
        let customer_state = SimpleOptionWrapper(customer_state);
        let c_cre_l = SimpleOptionWrapper(c_cre_l);
        let customer_discount = SimpleOptionWrapper(customer_discount);
        let customer_balance = SimpleOptionWrapper(customer_balance);
        let customer_credit = SimpleOptionWrapper(customer_credit);

        let (c_, c_last_____) = (customer_id, c_last_____);
        write!(
            f,
            r#"                                     Payment                                    
Date: {now}                                                       
                                                                                
Warehouse: {w:<6}                        District: {d:<2}                           
{w_street1_____:<20}                     {d_street1_____:<20}                   
{w_street2_____:<20}                     {d_street2_____:<20}                   
{warehouse_city:<20} {warehouse_state:<2} {w_zip}       {district_city:<20} {district_state:<2} {d_zip}     
                                                                                
Customer: {c_:<4}  Cust-Warehouse: {cw:<4}  Cust-District: {cd:<2}                         
Name:   {c_last_____:16} {customer_middle_name:<2} {customer_first_name:<16}     Since:  {c_since}             
        {c_street1_____:<20}                     Credit: {customer_credit}                     
        {c_street2_____:<20}                     %Disc:  {customer_discount:<5.2}                  
        {customer_city_:<20} {customer_state:<2} {c_zip:<10}       Phone:  {c_phone:<19}    
                                                                                
Amount Paid:          ${a:4.2}      New Cust-Balance: ${customer_balance:<14.2}           
Credit Limit:   ${c_cre_l:<13.2}                                                  
                                                                                
Cust-Data: {c_data1:<50}                   
           {c_data2:<50}                   
           {c_data3:<50}                   
           {c_data4:<50}                   
                                                                                
                                                                                
                                                                                "#
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use time::OffsetDateTime;

    use crate::tpcc::transaction::{test::terminal_display, Payment};

    use super::PaymentOut;

    #[test]
    fn display() {
        terminal_display(Payment::generate(29, 30, 29));
    }

    #[test]
    fn display_out() {
        terminal_display(PaymentOut {
            warehouse_id: 1,
            district_id: 2,
            customer_id: 3,
            customer_warehouse_id: 4,
            customer_district_id: 5,
            amount: 101.20,
            date: OffsetDateTime::now_utc(),
            warehouse_street: ("GOOGLE".to_string(), "MICROSOFT".to_string()),
            warehouse_city: "New York".to_string(),
            warehouse_state: "ST".to_string(),
            warehouse_zip: "888884444".to_string(),
            district_street: ("GOOGLE".to_string(), "MICROSOFT".to_string()),
            district_city: "New York".to_string(),
            district_state: "ST".to_string(),
            district_zip: "888884444".to_string(),
            customer_first_name: Some("WAYNE".to_string()),
            customer_last_name: Some("BRUCE".to_string()),
            customer_middle_name: Some("BR".to_string()),
            customer_street: (Some("GOOGLE".to_string()), Some("MICROSOFT".to_string())),
            customer_city: Some("GOTHAM".to_string()),
            customer_state: Some("GT".to_string()),
            customer_zip: Some("444488585".to_string()),
            customer_phone: Some("1234567890123456".to_string()),
            customer_since: OffsetDateTime::now_utc(),
            customer_credit: Some("GC".to_string()),
            customer_credit_lim: Some(123.45),
            customer_discount: Some(90.0),
            customer_balance: Some(999999999999.99),
            customer_data: Some("G".to_string().repeat(200)),
        });
    }
}
