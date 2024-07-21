use std::ptr::null_mut;

use rsqlbench_core::tpcc::{
    sut::Terminal,
    transaction::{
        CustomerSelector, Delivery, DeliveryOut, NewOrder, NewOrderLineOut, NewOrderOut,
        NewOrderRollbackOut, OrderStatus, OrderStatusOut, Payment, PaymentOut, StockLevel,
        StockLevelOut,
    },
};
use time::OffsetDateTime;
use tokio::task::spawn_blocking;
use tracing::trace;

use crate::{
    guard_yac_call,
    native::{
        yacBindParameter, yacExecute, yacPrepare, EnYacExtType_YAC_SQLT_FLOAT,
        EnYacExtType_YAC_SQLT_INTEGER, EnYacExtType_YAC_SQLT_VARCHAR2,
        EnYacParamDirection_YAC_PARAM_OUTPUT, YacParamDirection, YacUint16, YacUint32,
    },
    wrapper::{Error, StatementHandle},
    Connection,
};

pub struct YasdbTerminal {
    conn: Connection,
    warehouse_count: u32,
}

impl YasdbTerminal {
    pub fn new(conn: Connection, warehouse_count: u32) -> Self {
        Self {
            conn,
            warehouse_count,
        }
    }
}

#[async_trait::async_trait]
impl Terminal for YasdbTerminal {
    async fn new_order(
        &mut self,
        input: &NewOrder,
    ) -> anyhow::Result<Result<NewOrderOut, NewOrderRollbackOut>> {
        let stmt = StatementHandle::new(&self.conn.conn_handle)?;
        let warehouse_count = self.warehouse_count;
        let input = unsafe { &*(input as *const NewOrder) };
        let executed = spawn_blocking(move || ->Result<Result<NewOrderOut, NewOrderRollbackOut>, Error>{
            let NewOrder {
                warehouse_id,
                district_id,
                rollback_last,
                customer_id,
                order_lines,
            } = input;
            let ol_count = order_lines.len();
            let stmt = stmt;
            let mut discount = 0.0f32;
            let mut lastname = [0u8; 16];
            let mut credit = [0u8; 3];
            let mut d_tax = 0.0f32;
            let mut w_tax = 0.0f32;
            let mut next_order_id = 0u32;
            unsafe {
                let sql = format!("CALL NEWORD({warehouse_id}, {warehouse_count}, {district_id}, {customer_id}, {ol_count}, {rollback_last}, ?, ?, ?, ?, ?, ?, now())");
                trace!(sql);
                guard_yac_call!(yacPrepare(stmt.0, sql.as_ptr() as _, sql.len() as _))?;
                yac_bind_parameter(&stmt, 1, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_FLOAT, &mut discount)?;
                yac_bind_parameter_buffer(&stmt, 2, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut lastname)?;
                yac_bind_parameter_buffer(&stmt, 3, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut credit)?;
                yac_bind_parameter(&stmt, 4, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_FLOAT, &mut d_tax)?;
                yac_bind_parameter(&stmt, 5, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_FLOAT, &mut w_tax)?;
                yac_bind_parameter(&stmt, 6, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_INTEGER, &mut next_order_id)?;
                guard_yac_call!(yacExecute(stmt.0))?;
            };
            if !*rollback_last{
                Ok(Ok(NewOrderOut{ warehouse_id: *warehouse_id, district_id: *district_id, customer_id: *customer_id, discount, credit: String::from_utf8_lossy(&credit).to_string(), customer_last_name: String::from_utf8_lossy(&lastname).to_string(), warehouse_tax: w_tax, district_tax: d_tax, order_id: next_order_id, order_lines: vec![NewOrderLineOut{ item_id: 0, warehouse_id: *warehouse_id, quantity: 0, item_name: "Unimplemented now".to_string(), stock_quantity: 0, brand_generic: "G".to_string(), price: 0.0, amount: 0.29 }], entry_date: OffsetDateTime::now_utc() }))
            } else {
                Ok(Err(NewOrderRollbackOut{ warehouse_id: *warehouse_id, district_id: *district_id, customer_id: *customer_id, credit: String::from_utf8_lossy(&credit).to_string(), customer_last_name: String::from_utf8_lossy(&lastname).to_string(), order_id: next_order_id }))
            }
        })
        .await??;
        Ok(executed)
    }

    async fn payment(&mut self, input: &Payment) -> anyhow::Result<PaymentOut> {
        let stmt = StatementHandle::new(&self.conn.conn_handle)?;
        let input = unsafe { &*(input as *const Payment) };
        let r = spawn_blocking(move || ->Result<PaymentOut, Error> {
            let Payment {warehouse_id,district_id,customer,amount, .. } = input;
            let c_w_id = input.customer_warehouse_id();
            let stmt = stmt;
            let (by_name, c_last_name, mut customer_id) = match customer {
                CustomerSelector::LastName(n) => (1, n.as_str(), 0),
                CustomerSelector::ID(id) => (0, "", *id),
            };

            let mut customer_first_name = [0u8;17];
            let mut customer_last_name = [0u8;16];
            let mut customer_middle_name = [0u8;3];
            let mut customer_street1 = [0u8;21];
            let mut customer_street2 = [0u8;21];
            let mut customer_city = [0u8;21];
            let mut customer_state = [0u8;3];
            let mut customer_zip = [0u8;10];
            let mut customer_phone = [0u8;17];
            let mut customer_credit = [0u8;3];
            let mut customer_credit_limit = 0.0f32;
            let mut customer_discount = 0.0f32;
            let mut customer_balance = 0.0f32;
            let mut customer_data = [0u8; 501];
            let mut wh_street1 = [0u8;21];
            let mut wh_street2 = [0u8;21];
            let mut wh_city = [0u8;21];
            let mut wh_state = [0u8;3];
            let mut wh_zip = [0u8;10];
            let mut d_street1 = [0u8;21];
            let mut d_street2 = [0u8;21];
            let mut d_city = [0u8;21];
            let mut d_state = [0u8;3];
            let mut d_zip = [0u8;10];
            let mut since = [0u8;20];

            customer_last_name[0..c_last_name.len()].copy_from_slice(c_last_name.as_bytes());

            unsafe {
                let sql = format!(r"CALL PAYMENT({warehouse_id}, {district_id}, {c_w_id}, {district_id}, ?, {by_name}, {amount}, ?,
                                                        ?, ?, ?, ?, ?,
                                                        ?, ?, ?, ?, ?,
                                                        ?, ?,
                                                        ?, ?, ?, ?, ?,
                                                        ?, ?, ?, ?, ?,
                                                        ?, ?,
                                                        now())");
                trace!(sql);
                guard_yac_call!(yacPrepare(stmt.0, sql.as_ptr() as _, sql.len() as _))?;
                yac_bind_parameter(&stmt, 1, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_INTEGER, &mut customer_id)?;
                yac_bind_parameter_buffer(&stmt, 2, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut customer_last_name)?;
                yac_bind_parameter_buffer(&stmt, 3, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut wh_street1)?;
                yac_bind_parameter_buffer(&stmt, 4, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut wh_street2)?;
                yac_bind_parameter_buffer(&stmt, 5, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut wh_city)?;
                yac_bind_parameter_buffer(&stmt, 6, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut wh_state)?;
                yac_bind_parameter_buffer(&stmt, 7, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut wh_zip)?;
                yac_bind_parameter_buffer(&stmt, 8, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut d_street1)?;
                yac_bind_parameter_buffer(&stmt, 9, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut d_street2)?;
                yac_bind_parameter_buffer(&stmt, 10, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut d_city)?;
                yac_bind_parameter_buffer(&stmt, 11, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut d_state)?;
                yac_bind_parameter_buffer(&stmt, 12, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut d_zip)?;
                yac_bind_parameter_buffer(&stmt, 13, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut customer_first_name)?;
                yac_bind_parameter_buffer(&stmt, 14, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut customer_middle_name)?;
                yac_bind_parameter_buffer(&stmt, 15, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut customer_street1)?;
                yac_bind_parameter_buffer(&stmt, 16, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut customer_street2)?;
                yac_bind_parameter_buffer(&stmt, 17, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut customer_city)?;
                yac_bind_parameter_buffer(&stmt, 18, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut customer_state)?;
                yac_bind_parameter_buffer(&stmt, 19, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut customer_zip)?;
                yac_bind_parameter_buffer(&stmt, 20, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut customer_phone)?;
                yac_bind_parameter_buffer(&stmt, 21, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut since)?;
                yac_bind_parameter_buffer(&stmt, 22, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_VARCHAR2, &mut customer_credit)?;
                yac_bind_parameter(&stmt, 23, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_FLOAT, &mut customer_credit_limit)?;
                yac_bind_parameter(&stmt, 24, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_FLOAT, &mut customer_discount)?;
                yac_bind_parameter(&stmt, 25, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_FLOAT, &mut customer_balance)?;
                yac_bind_parameter(&stmt, 26, EnYacParamDirection_YAC_PARAM_OUTPUT, EnYacExtType_YAC_SQLT_FLOAT, &mut customer_data)?;
                let result = guard_yac_call!(yacExecute(stmt.0));
                match result {
                    // TODO cargo fmt doesn't take affects here
                    Ok(_) => Ok(PaymentOut{ warehouse_id: *warehouse_id, district_id: *district_id, customer_id, customer_warehouse_id: c_w_id, customer_district_id: *district_id, amount: *amount, date: OffsetDateTime::now_utc(), warehouse_street: (String::from_utf8_lossy(&wh_street1).to_string(), String::from_utf8_lossy(&wh_street2).to_string()), warehouse_city: String::from_utf8_lossy(&wh_city).to_string(), warehouse_state: String::from_utf8_lossy(&wh_state).to_string(), warehouse_zip: String::from_utf8_lossy(&wh_zip).to_string(), district_street: (String::from_utf8_lossy(&d_street1).to_string(),String::from_utf8_lossy(&d_street2).to_string()), district_city: String::from_utf8_lossy(&d_city).to_string(), district_state: String::from_utf8_lossy(&d_state).to_string(), district_zip: String::from_utf8_lossy(&d_zip).to_string(), customer_first_name: String::from_utf8_lossy(&customer_first_name).to_string().into(), customer_last_name: String::from_utf8_lossy(&customer_last_name).to_string().into(), customer_middle_name: String::from_utf8_lossy(&customer_middle_name).to_string().into(), customer_street: (String::from_utf8_lossy(&customer_street1).to_string().into(),String::from_utf8_lossy(&customer_street2).to_string().into()), customer_city: String::from_utf8_lossy(&customer_city).to_string().into(), customer_state: String::from_utf8_lossy(&customer_state).to_string().into(), customer_zip: String::from_utf8_lossy(&customer_zip).to_string().into(), customer_phone: String::from_utf8_lossy(&customer_phone).to_string().into(), customer_since: OffsetDateTime::now_utc(), customer_credit: String::from_utf8_lossy(&customer_credit).to_string().into(), customer_credit_lim: Some(customer_credit_limit), customer_discount: Some(customer_discount), customer_balance: Some(customer_balance), customer_data: String::from_utf8_lossy(&customer_data).to_string().into() }),
                    Err(Error::YasClient(err)) if err.code == 5206 => Ok(PaymentOut{ warehouse_id: *warehouse_id, district_id: *district_id, customer_id, customer_warehouse_id: c_w_id, customer_district_id: *district_id, amount: *amount, date: OffsetDateTime::now_utc(), warehouse_street: (String::from_utf8_lossy(&wh_street1).to_string(), String::from_utf8_lossy(&wh_street2).to_string()), warehouse_city: String::from_utf8_lossy(&wh_city).to_string(), warehouse_state: String::from_utf8_lossy(&wh_state).to_string(), warehouse_zip: String::from_utf8_lossy(&wh_zip).to_string(), district_street: (String::from_utf8_lossy(&d_street1).to_string(),String::from_utf8_lossy(&d_street2).to_string()), district_city: String::from_utf8_lossy(&d_city).to_string(), district_state: String::from_utf8_lossy(&d_state).to_string(), district_zip: String::from_utf8_lossy(&d_zip).to_string(), customer_first_name: None, customer_last_name: None, customer_middle_name: None, customer_street: (None, None), customer_city: None, customer_state: None, customer_zip: None, customer_phone: None, customer_since: OffsetDateTime::now_utc(), customer_credit: None, customer_credit_lim: None, customer_discount: None, customer_balance: None, customer_data: None }),
                    Err(e)=> Err(e)
                }
            }
        })
        .await??;
        Ok(r)
    }

    async fn order_status(&mut self, _input: &OrderStatus) -> anyhow::Result<OrderStatusOut> {
        unimplemented!()
    }

    async fn delivery(&mut self, _input: &Delivery) -> anyhow::Result<DeliveryOut> {
        unimplemented!()
    }

    async fn stock_level(&mut self, _input: &StockLevel) -> anyhow::Result<StockLevelOut> {
        unimplemented!()
    }
}

unsafe fn yac_bind_parameter<T: Sized>(
    stmt: &StatementHandle,
    id: YacUint16,
    direction: YacParamDirection,
    ext_type: YacUint32,
    value: &mut T,
) -> Result<(), Error> {
    guard_yac_call!(yacBindParameter(
        stmt.0,
        id,
        direction,
        ext_type,
        value as *mut T as _,
        std::mem::size_of::<T>() as _,
        std::mem::size_of::<T>() as _,
        null_mut()
    ))
}

unsafe fn yac_bind_parameter_buffer(
    stmt: &StatementHandle,
    id: YacUint16,
    direction: YacParamDirection,
    ext_type: YacUint32,
    value: &mut [u8],
) -> Result<(), Error> {
    guard_yac_call!(yacBindParameter(
        stmt.0,
        id,
        direction,
        ext_type,
        value.as_ptr() as *mut u8 as _,
        value.len() as _,
        value.len() as _,
        null_mut()
    ))
}
