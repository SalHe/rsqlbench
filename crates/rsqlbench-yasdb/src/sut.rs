use rsqlbench_core::{
    cfg::Connection as ConnectionCfg,
    tpcc::{
        loader::Loader,
        sut::{generic_direct::Executor, Sut, Terminal},
    },
};
use tokio::task::spawn_blocking;
use tracing::{info, warn};

use crate::{
    native::{yacConnect, EnYacResult_YAC_ERROR},
    wrapper::{DbcHandle, EnvHandle, Error, SimpleExecutor, StatementHandle},
    Connection, YasdbLoader,
};

pub struct YasdbSut {
    connection: ConnectionCfg,
}

impl YasdbSut {
    pub fn new(connection: ConnectionCfg) -> Self {
        Self { connection }
    }

    async fn connect(&self, url: String) -> anyhow::Result<Connection> {
        let username = self.connection.connections.others["username"].clone();
        let password = self.connection.connections.others["password"].clone();
        let conn = spawn_blocking(move || {
            let env_handle = EnvHandle::new()?.with_utf8();
            let conn_handle = DbcHandle::new(&env_handle)?;
            let result = unsafe {
                yacConnect(
                    conn_handle.0,
                    url.as_ptr() as _,
                    url.len() as _,
                    username.as_ptr() as _,
                    username.len() as _,
                    password.as_ptr() as _,
                    password.len() as _,
                )
            };

            if result == EnYacResult_YAC_ERROR {
                return Err(Error::get_yas_diag(None).unwrap());
            }
            Ok(Connection {
                conn_handle,
                _env_handle: env_handle,
            })
        })
        .await?;
        Ok(conn?)
    }
}

#[async_trait::async_trait]
impl Sut for YasdbSut {
    async fn terminal(&self, _id: u32) -> anyhow::Result<Box<dyn Terminal>> {
        unimplemented!()
    }

    async fn build_schema(&self) -> anyhow::Result<()> {
        let conn = self
            .connect(self.connection.connections.schema.to_string())
            .await?;
        // let db_ddl = format!("create database {}", self.connection.database);
        let sql_set = [
            // `drop database` is not supported, so database should be created manually.
            // db_ddl.as_str(),
            r"CREATE TABLE CUSTOMER (C_ID NUMBER(5, 0), C_D_ID NUMBER(2, 0), C_W_ID NUMBER(6, 0), C_FIRST VARCHAR2(16), C_MIDDLE CHAR(2), C_LAST VARCHAR2(16), C_STREET_1 VARCHAR2(20), C_STREET_2 VARCHAR2(20), C_CITY VARCHAR2(20), C_STATE CHAR(2), C_ZIP CHAR(9), C_PHONE CHAR(16), C_SINCE DATE, C_CREDIT CHAR(2), C_CREDIT_LIM NUMBER(12, 2), C_DISCOUNT NUMBER(4, 4), C_BALANCE NUMBER(12, 2), C_YTD_PAYMENT NUMBER(12, 2), C_PAYMENT_CNT NUMBER(8, 0), C_DELIVERY_CNT NUMBER(8, 0), C_DATA VARCHAR2(500))",
            r"CREATE TABLE DISTRICT (D_ID NUMBER(2, 0), D_W_ID NUMBER(6, 0), D_YTD NUMBER(12, 2), D_TAX NUMBER(4, 4), D_NEXT_O_ID NUMBER, D_NAME VARCHAR2(10), D_STREET_1 VARCHAR2(20), D_STREET_2 VARCHAR2(20), D_CITY VARCHAR2(20), D_STATE CHAR(2), D_ZIP CHAR(9))",
            r"CREATE TABLE HISTORY (H_C_ID NUMBER, H_C_D_ID NUMBER, H_C_W_ID NUMBER, H_D_ID NUMBER, H_W_ID NUMBER, H_DATE DATE, H_AMOUNT NUMBER(6, 2), H_DATA VARCHAR2(24)) ",
            r"CREATE TABLE ITEM (I_ID NUMBER(6, 0), I_IM_ID NUMBER, I_NAME VARCHAR2(24), I_PRICE NUMBER(5, 2), I_DATA VARCHAR2(50))",
            r"CREATE TABLE WAREHOUSE (W_ID NUMBER(6, 0), W_YTD NUMBER(12, 2), W_TAX NUMBER(4, 4), W_NAME VARCHAR2(10), W_STREET_1 VARCHAR2(20), W_STREET_2 VARCHAR2(20), W_CITY VARCHAR2(20), W_STATE CHAR(2), W_ZIP CHAR(9))",
            r"CREATE TABLE STOCK (S_I_ID NUMBER(6, 0), S_W_ID NUMBER(6, 0), S_QUANTITY NUMBER(6, 0), S_DIST_01 CHAR(24), S_DIST_02 CHAR(24), S_DIST_03 CHAR(24), S_DIST_04 CHAR(24), S_DIST_05 CHAR(24), S_DIST_06 CHAR(24), S_DIST_07 CHAR(24), S_DIST_08 CHAR(24), S_DIST_09 CHAR(24), S_DIST_10 CHAR(24), S_YTD NUMBER(10, 0), S_ORDER_CNT NUMBER(6, 0), S_REMOTE_CNT NUMBER(6, 0), S_DATA VARCHAR2(50))",
            r"CREATE TABLE NEW_ORDER (NO_W_ID NUMBER, NO_D_ID NUMBER, NO_O_ID NUMBER, CONSTRAINT INORD PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID) ENABLE)",
            r"CREATE TABLE OORDER (O_ID NUMBER, O_W_ID NUMBER, O_D_ID NUMBER, O_C_ID NUMBER, O_CARRIER_ID NUMBER, O_OL_CNT NUMBER, O_ALL_LOCAL NUMBER, O_ENTRY_D DATE)",
            r"CREATE TABLE ORDER_LINE (OL_W_ID NUMBER, OL_D_ID NUMBER, OL_O_ID NUMBER, OL_NUMBER NUMBER, OL_I_ID NUMBER, OL_DELIVERY_D DATE, OL_AMOUNT NUMBER, OL_SUPPLY_W_ID NUMBER, OL_QUANTITY NUMBER, OL_DIST_INFO CHAR(24), CONSTRAINT IORDL PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER) ENABLE)",
        ];
        let mut exec = SimpleExecutor::new(StatementHandle::new(&conn.conn_handle)?);
        for sql in sql_set {
            info!(ddl = sql, "Creating table");
            exec.execute(sql).await?;
        }
        Ok(())
    }

    async fn after_loaded(&self) -> anyhow::Result<()> {
        let conn = self
            .connect(self.connection.connections.schema.to_string())
            .await?;
        let mut exec = SimpleExecutor::new(StatementHandle::new(&conn.conn_handle)?);

        // Build indexes
        let sql_set = [
            "CREATE UNIQUE INDEX CUSTOMER_I1 ON CUSTOMER (C_W_ID, C_D_ID, C_ID)",
            "CREATE UNIQUE INDEX CUSTOMER_I2 ON CUSTOMER (C_LAST, C_D_ID, C_W_ID, C_FIRST)",
            "CREATE UNIQUE INDEX DISTRICT_I1 ON DISTRICT (D_W_ID, D_ID)",
            "CREATE UNIQUE INDEX ITEM_I1 ON ITEM (I_ID)",
            "CREATE UNIQUE INDEX OORDER_I1 ON OORDER (O_W_ID, O_D_ID, O_ID)",
            "CREATE UNIQUE INDEX OORDER_I2 ON OORDER (O_W_ID, O_D_ID, O_C_ID, O_ID)",
            "CREATE UNIQUE INDEX STOCK_I1 ON STOCK (S_I_ID, S_W_ID)",
            "CREATE UNIQUE INDEX WAREHOUSE_I1 ON WAREHOUSE (W_ID)",
        ];
        info!("Building indexes...");
        for sql in sql_set {
            exec.execute(sql).await?;
        }
        info!("Indexes created.");

        // Create procedures
        let sql_set = [
            r"CREATE OR REPLACE PROCEDURE NEWORD (
            no_w_id		BINARY_INTEGER,
            no_max_w_id		BINARY_INTEGER,
            no_d_id		    BINARY_INTEGER,
            no_c_id		    BINARY_INTEGER,
            no_o_ol_cnt		BINARY_INTEGER,
            no_c_discount	OUT NUMBER,
            no_c_last		OUT VARCHAR2,
            no_c_credit		OUT VARCHAR2,
            no_d_tax		OUT NUMBER,
            no_w_tax		OUT NUMBER,
            no_d_next_o_id	OUT BINARY_INTEGER,
            timestamp		IN DATE )
            IS
            order_amount        NUMBER;
            no_o_all_local		BINARY_INTEGER;
            loop_counter        BINARY_INTEGER;
            not_serializable		EXCEPTION;
            PRAGMA EXCEPTION_INIT(not_serializable,-8177);
            deadlock			EXCEPTION;
            PRAGMA EXCEPTION_INIT(deadlock,-60);
            snapshot_too_old		EXCEPTION;
            PRAGMA EXCEPTION_INIT(snapshot_too_old,-1555);
            integrity_viol			EXCEPTION;
            PRAGMA EXCEPTION_INIT(integrity_viol,-1);
            TYPE intarray IS TABLE OF INTEGER index by binary_integer;
            TYPE numarray IS TABLE OF NUMBER index by binary_integer;
            TYPE distarray IS TABLE OF VARCHAR(24) index by binary_integer;
            o_id_array intarray;
            w_id_array intarray;
            o_quantity_array intarray;
            s_quantity_array intarray;
            ol_line_number_array intarray;
            amount_array numarray;
            district_info distarray;
            BEGIN
            SELECT c_discount, c_last, c_credit, w_tax
            INTO no_c_discount, no_c_last, no_c_credit, no_w_tax
            FROM customer, warehouse
            WHERE warehouse.w_id = no_w_id AND customer.c_w_id = no_w_id AND customer.c_d_id = no_d_id AND customer.c_id = no_c_id;

            --#2.4.1.5
            no_o_all_local := 1;
            FOR loop_counter IN 1 .. no_o_ol_cnt
            LOOP
            o_id_array(loop_counter) := round(DBMS_RANDOM.value(low => 1, high => 100000));

            --#2.4.1.5.2
            IF ( DBMS_RANDOM.value >= 0.01 )
            THEN
            w_id_array(loop_counter) := no_w_id;
            ELSE
            no_o_all_local := 0;
            w_id_array(loop_counter) := 1 + mod(no_w_id + round(DBMS_RANDOM.value(low => 0, high => no_max_w_id-1)),no_max_w_id);
            END IF;

            --#2.4.1.5.3
            o_quantity_array(loop_counter) := round(DBMS_RANDOM.value(low => 1, high => 10));

            -- Take advantage of the fact that I'm looping to populate the array used to record order lines at the end
            ol_line_number_array(loop_counter) := loop_counter;
            END LOOP;

            UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = no_d_id AND d_w_id = no_w_id RETURNING d_next_o_id - 1, d_tax INTO no_d_next_o_id, no_d_tax;

            INSERT INTO OORDER (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (no_d_next_o_id, no_d_id, no_w_id, no_c_id, timestamp, no_o_ol_cnt, no_o_all_local);
            INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES (no_d_next_o_id, no_d_id, no_w_id);

            -- The HammerDB implementation doesn't do the check for ORIGINAL (which should be done against i_data and s_data)
            IF no_d_id = 1 THEN
            FORALL i IN 1 .. no_o_ol_cnt
            UPDATE stock_item
            SET s_quantity = (CASE WHEN s_quantity < ( o_quantity_array(i) + 10 ) THEN s_quantity + 91 ELSE s_quantity END) - o_quantity_array(i)
            WHERE i_id = o_id_array(i)
            AND s_w_id = w_id_array(i)
            AND i_id = o_id_array(i)
            RETURNING s_dist_01, s_quantity, i_price * o_quantity_array(i) BULK COLLECT INTO district_info, s_quantity_array, amount_array;
            ELSIF no_d_id = 2 THEN
            FORALL i IN 1 .. no_o_ol_cnt
            UPDATE stock_item
            SET s_quantity = (CASE WHEN s_quantity < ( o_quantity_array(i) + 10 ) THEN s_quantity + 91 ELSE s_quantity END) - o_quantity_array(i)
            WHERE i_id = o_id_array(i)
            AND s_w_id = w_id_array(i)
            AND i_id = o_id_array(i)
            RETURNING s_dist_02, s_quantity, i_price * o_quantity_array(i) BULK COLLECT INTO district_info, s_quantity_array,amount_array;
            ELSIF no_d_id = 3 THEN
            FORALL i IN 1 .. no_o_ol_cnt
            UPDATE stock_item
            SET s_quantity = (CASE WHEN s_quantity < ( o_quantity_array(i) + 10 ) THEN s_quantity + 91 ELSE s_quantity END) - o_quantity_array(i)
            WHERE i_id = o_id_array(i)
            AND s_w_id = w_id_array(i)
            AND i_id = o_id_array(i)
            RETURNING s_dist_03, s_quantity, i_price * o_quantity_array(i) BULK COLLECT INTO district_info, s_quantity_array,amount_array;
            ELSIF no_d_id = 4 THEN
            FORALL i IN 1 .. no_o_ol_cnt
            UPDATE stock_item
            SET s_quantity = (CASE WHEN s_quantity < ( o_quantity_array(i) + 10 ) THEN s_quantity + 91 ELSE s_quantity END) - o_quantity_array(i)
            WHERE i_id = o_id_array(i)
            AND s_w_id = w_id_array(i)
            AND i_id = o_id_array(i)
            RETURNING s_dist_04, s_quantity, i_price * o_quantity_array(i) BULK COLLECT INTO district_info, s_quantity_array,amount_array;
            ELSIF no_d_id = 5 THEN
            FORALL i IN 1 .. no_o_ol_cnt
            UPDATE stock_item
            SET s_quantity = (CASE WHEN s_quantity < ( o_quantity_array(i) + 10 ) THEN s_quantity + 91 ELSE s_quantity END) - o_quantity_array(i)
            WHERE i_id = o_id_array(i)
            AND s_w_id = w_id_array(i)
            AND i_id = o_id_array(i)
            RETURNING s_dist_05, s_quantity, i_price * o_quantity_array(i) BULK COLLECT INTO district_info, s_quantity_array,amount_array;
            ELSIF no_d_id = 6 THEN
            FORALL i IN 1 .. no_o_ol_cnt
            UPDATE stock_item
            SET s_quantity = (CASE WHEN s_quantity < ( o_quantity_array(i) + 10 ) THEN s_quantity + 91 ELSE s_quantity END) - o_quantity_array(i)
            WHERE i_id = o_id_array(i)
            AND s_w_id = w_id_array(i)
            AND i_id = o_id_array(i)
            RETURNING s_dist_06, s_quantity, i_price * o_quantity_array(i) BULK COLLECT INTO district_info, s_quantity_array,amount_array;
            ELSIF no_d_id = 7 THEN
            FORALL i IN 1 .. no_o_ol_cnt
            UPDATE stock_item
            SET s_quantity = (CASE WHEN s_quantity < ( o_quantity_array(i) + 10 ) THEN s_quantity + 91 ELSE s_quantity END) - o_quantity_array(i)
            WHERE i_id = o_id_array(i)
            AND s_w_id = w_id_array(i)
            AND i_id = o_id_array(i)
            RETURNING s_dist_07, s_quantity, i_price * o_quantity_array(i) BULK COLLECT INTO district_info, s_quantity_array,amount_array;
            ELSIF no_d_id = 8 THEN
            FORALL i IN 1 .. no_o_ol_cnt
            UPDATE stock_item
            SET s_quantity = (CASE WHEN s_quantity < ( o_quantity_array(i) + 10 ) THEN s_quantity + 91 ELSE s_quantity END) - o_quantity_array(i)
            WHERE i_id = o_id_array(i)
            AND s_w_id = w_id_array(i)
            AND i_id = o_id_array(i)
            RETURNING s_dist_08, s_quantity, i_price * o_quantity_array(i) BULK COLLECT INTO district_info, s_quantity_array,amount_array;
            ELSIF no_d_id = 9 THEN
            FORALL i IN 1 .. no_o_ol_cnt
            UPDATE stock_item
            SET s_quantity = (CASE WHEN s_quantity < ( o_quantity_array(i) + 10 ) THEN s_quantity + 91 ELSE s_quantity END) - o_quantity_array(i)
            WHERE i_id = o_id_array(i)
            AND s_w_id = w_id_array(i)
            AND i_id = o_id_array(i)
            RETURNING s_dist_09, s_quantity, i_price * o_quantity_array(i) BULK COLLECT INTO district_info, s_quantity_array,amount_array;
            ELSIF no_d_id = 10 THEN
            FORALL i IN 1 .. no_o_ol_cnt
            UPDATE stock_item
            SET s_quantity = (CASE WHEN s_quantity < ( o_quantity_array(i) + 10 ) THEN s_quantity + 91 ELSE s_quantity END) - o_quantity_array(i)
            WHERE i_id = o_id_array(i)
            AND s_w_id = w_id_array(i)
            AND i_id = o_id_array(i)
            RETURNING s_dist_10, s_quantity, i_price * o_quantity_array(i) BULK COLLECT INTO district_info, s_quantity_array,amount_array;
            END IF;

            -- Oracle return the TAX information to the client, presumably to do the calculation there.  HammerDB doesn't return it at all so I'll just calculate it here and do nothing with it
            order_amount := 0;
            FOR loop_counter IN 1 .. no_o_ol_cnt
            LOOP
            order_amount := order_amount + ( amount_array(loop_counter) );
            END LOOP;
            order_amount := order_amount * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount );

            FORALL i IN 1 .. no_o_ol_cnt
            INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
            VALUES (no_d_next_o_id, no_d_id, no_w_id, ol_line_number_array(i), o_id_array(i), w_id_array(i), o_quantity_array(i), amount_array(i), district_info(i));

            -- Rollback 1% of transactions
            IF DBMS_RANDOM.value < 0.01 THEN
            dbms_output.put_line('Rolling back');
            ROLLBACK;
            ELSE
            COMMIT;
            END IF;

            EXCEPTION
            WHEN not_serializable OR deadlock OR snapshot_too_old OR integrity_viol --OR no_data_found
            THEN
            ROLLBACK;
        END;",
            r"CREATE OR REPLACE PROCEDURE PAYMENT (
        p_w_id			INTEGER,
        p_d_id			INTEGER,
        p_c_w_id		INTEGER,
        p_c_d_id		INTEGER,
        p_c_id			IN OUT INTEGER,
        byname			INTEGER,
        p_h_amount		NUMBER,
        p_c_last		IN OUT VARCHAR2,
        p_w_street_1	OUT VARCHAR2,
        p_w_street_2	OUT VARCHAR2,
        p_w_city		OUT VARCHAR2,
        p_w_state		OUT VARCHAR2,
        p_w_zip			OUT VARCHAR2,
        p_d_street_1	OUT VARCHAR2,
        p_d_street_2	OUT VARCHAR2,
        p_d_city		OUT VARCHAR2,
        p_d_state		OUT VARCHAR2,
        p_d_zip			OUT VARCHAR2,
        p_c_first		OUT VARCHAR2,
        p_c_middle		OUT VARCHAR2,
        p_c_street_1	OUT VARCHAR2,
        p_c_street_2	OUT VARCHAR2,
        p_c_city		OUT VARCHAR2,
        p_c_state		OUT VARCHAR2,
        p_c_zip			OUT VARCHAR2,
        p_c_phone		OUT VARCHAR2,
        p_c_since		OUT DATE,
        p_c_credit		IN OUT VARCHAR2,
        p_c_credit_lim	OUT NUMBER,
        p_c_discount	OUT NUMBER,
        p_c_balance		IN OUT NUMBER,
        p_c_data		OUT VARCHAR2,
        timestamp		IN DATE
        )
        IS
        p_d_name		VARCHAR2(11);
        p_w_name		VARCHAR2(11);
        p_c_new_data	VARCHAR2(500);
        h_data			VARCHAR2(30);

        TYPE rowidarray IS TABLE OF ROWID INDEX BY BINARY_INTEGER;
        cust_rowid ROWID;
        row_id rowidarray;
        c_num BINARY_INTEGER;

        CURSOR c_byname IS
        SELECT rowid
        FROM customer
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_last = p_c_last
        ORDER BY c_first;

        not_serializable		EXCEPTION;
        PRAGMA EXCEPTION_INIT(not_serializable,-8177);
        deadlock			EXCEPTION;
        PRAGMA EXCEPTION_INIT(deadlock,-60);
        snapshot_too_old		EXCEPTION;
        PRAGMA EXCEPTION_INIT(snapshot_too_old,-1555);

        BEGIN
        IF ( byname = 1 )
        THEN
        c_num := 0;
        FOR c_id_rec IN c_byname LOOP
        c_num := c_num + 1;
        row_id(c_num) := c_id_rec.rowid;
        END LOOP;
        cust_rowid := row_id ((c_num + 1) / 2);

        UPDATE customer
        SET c_balance = c_balance - p_h_amount
        --c_ytd_payment = c_ytd_payment + hist_amount,
        --c_payment_cnt = c_payment_cnt + 1
        WHERE rowid = cust_rowid
        RETURNING c_id, c_first, c_middle, c_last, c_street_1, c_street_2,
        c_city, c_state, c_zip, c_phone,
        c_since, c_credit, c_credit_lim,
        c_discount, c_balance
        INTO p_c_id, p_c_first, p_c_middle, p_c_last, p_c_street_1, p_c_street_2,
        p_c_city, p_c_state, p_c_zip, p_c_phone,
        p_c_since, p_c_credit, p_c_credit_lim,
        p_c_discount, p_c_balance;
        ELSE
        UPDATE customer
        SET c_balance = c_balance - p_h_amount
        --c_ytd_payment = c_ytd_payment + hist_amount,
        --c_payment_cnt = c_payment_cnt + 1
        WHERE c_id = p_c_id AND c_d_id = p_c_d_id AND c_w_id = p_c_w_id
        RETURNING rowid, c_first, c_middle, c_last, c_street_1, c_street_2,
        c_city, c_state, c_zip, c_phone,
        c_since, c_credit, c_credit_lim,
        c_discount, c_balance
        INTO cust_rowid, p_c_first, p_c_middle, p_c_last, p_c_street_1, p_c_street_2,
        p_c_city, p_c_state, p_c_zip, p_c_phone,
        p_c_since, p_c_credit, p_c_credit_lim,
        p_c_discount, p_c_balance;
        END IF;

        IF p_c_credit = 'BC' THEN
        UPDATE customer
        SET c_data = substr ((to_char (p_c_id) || ' ' ||
        to_char (p_c_d_id) || ' ' ||
        to_char (p_c_w_id) || ' ' ||
        to_char (p_d_id) || ' ' ||
        to_char (p_w_id) || ' ' ||
        to_char (p_h_amount, '9999.99') || ' | ') || c_data, 1, 500)
        WHERE rowid = cust_rowid
        RETURNING substr (c_data, 1, 200) INTO p_c_data;
        ELSE
        p_c_data := ' ';
        END IF;

        UPDATE district
        SET d_ytd = d_ytd + p_h_amount
        WHERE d_id = p_d_id
        AND d_w_id = p_w_id
        RETURNING d_name, d_street_1, d_street_2, d_city,d_state, d_zip
        INTO p_d_name, p_d_street_1, p_d_street_2, p_d_city, p_d_state, p_d_zip;

        UPDATE warehouse
        SET w_ytd = w_ytd + p_h_amount
        WHERE w_id = p_w_id
        RETURNING w_name, w_street_1, w_street_2, w_city, w_state, w_zip
        INTO p_w_name, p_w_street_1, p_w_street_2, p_w_city, p_w_state, p_w_zip;

        INSERT INTO history
        (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date,h_amount,h_data)
        VALUES
        (p_c_id, p_c_d_id, p_c_w_id, p_d_id, p_w_id, timestamp, p_h_amount, p_w_name || ' ' || p_d_name);

        COMMIT;

        EXCEPTION
        WHEN not_serializable OR deadlock OR snapshot_too_old
        THEN
        ROLLBACK;
    END",
            r"CREATE OR REPLACE PROCEDURE DELIVERY (
            d_w_id			INTEGER,
            d_o_carrier_id	INTEGER,
            timestamp		DATE
            )
            IS
            TYPE intarray IS TABLE OF INTEGER index by binary_integer;
            dist_id_in_array    intarray;
            dist_id_array       intarray;
            o_id_array          intarray;
            order_c_id          intarray;
            sums                intarray;
            ordcnt              INTEGER;

            not_serializable		EXCEPTION;
            PRAGMA EXCEPTION_INIT(not_serializable,-8177);
            deadlock			EXCEPTION;
            PRAGMA EXCEPTION_INIT(deadlock,-60);
            snapshot_too_old		EXCEPTION;
            PRAGMA EXCEPTION_INIT(snapshot_too_old,-1555);
            BEGIN
            FOR i in 1 .. 10 LOOP
            dist_id_in_array(i) := i;
            END LOOP;

            FORALL d IN 1..10
            DELETE
            FROM new_order
            WHERE no_d_id = dist_id_in_array(d)
            AND no_w_id = d_w_id
            AND no_o_id = (select min (no_o_id)
            from new_order
            where no_d_id = dist_id_in_array(d)
            and no_w_id = d_w_id)
            RETURNING no_d_id, no_o_id BULK COLLECT INTO dist_id_array, o_id_array;

            ordcnt := SQL%ROWCOUNT;

            FORALL o in 1.. ordcnt
            UPDATE OORDER
            SET o_carrier_id = d_o_carrier_id
            WHERE o_id = o_id_array (o)
            AND o_d_id = dist_id_array(o)
            AND o_w_id = d_w_id
            RETURNING o_c_id BULK COLLECT INTO order_c_id;

            FORALL o in 1.. ordcnt
            UPDATE order_line
            SET ol_delivery_d = timestamp
            WHERE ol_w_id = d_w_id
            AND ol_d_id = dist_id_array(o)
            AND ol_o_id = o_id_array (o)
            RETURNING sum(ol_amount) BULK COLLECT INTO sums;

            FORALL c IN 1.. ordcnt
            UPDATE customer
            SET c_balance = c_balance + sums(c)
            -- Added this in for the refactor but it's not in the original (although it should be) so I've removed it, to be true to the original
            --, c_delivery_cnt = c_delivery_cnt + 1
            WHERE c_w_id = d_w_id
            AND c_d_id = dist_id_array(c)
            AND c_id = order_c_id(c);

            COMMIT;

            EXCEPTION
            WHEN not_serializable OR deadlock OR snapshot_too_old THEN
            ROLLBACK;
        END",
            r"CREATE OR REPLACE PROCEDURE OSTAT (
        os_w_id			INTEGER,
        os_d_id			INTEGER,
        os_c_id			IN OUT INTEGER,
        byname			INTEGER,
        os_c_last		IN OUT VARCHAR2,
        os_c_first		OUT VARCHAR2,
        os_c_middle		OUT VARCHAR2,
        os_c_balance		OUT NUMBER,
        os_o_id			OUT INTEGER,
        os_entdate		OUT DATE,
        os_o_carrier_id		OUT INTEGER
        )
        IS
        TYPE rowidarray IS TABLE OF ROWID INDEX BY BINARY_INTEGER;
        cust_rowid ROWID;
        row_id rowidarray;
        c_num BINARY_INTEGER;

        CURSOR c_byname
        IS
        SELECT rowid
        FROM customer
        WHERE c_w_id = os_w_id AND c_d_id = os_d_id AND c_last = os_c_last
        ORDER BY c_first;

        i			BINARY_INTEGER;
        CURSOR c_line IS
        SELECT ol_i_id, ol_supply_w_id, ol_quantity,
        ol_amount, ol_delivery_d
        FROM order_line
        WHERE ol_o_id = os_o_id AND ol_d_id = os_d_id AND ol_w_id = os_w_id;


        TYPE intarray IS TABLE OF INTEGER index by binary_integer;
        os_ol_i_id intarray;
        os_ol_supply_w_id intarray;
        os_ol_quantity intarray;

        TYPE datetable IS TABLE OF DATE INDEX BY BINARY_INTEGER;
        os_ol_delivery_d datetable;

        TYPE numarray IS TABLE OF NUMBER index by binary_integer;
        os_ol_amount numarray;

        not_serializable		EXCEPTION;
        PRAGMA EXCEPTION_INIT(not_serializable,-8177);
        deadlock			EXCEPTION;
        PRAGMA EXCEPTION_INIT(deadlock,-60);
        snapshot_too_old		EXCEPTION;
        PRAGMA EXCEPTION_INIT(snapshot_too_old,-1555);
        BEGIN
        IF ( byname = 1 )
        THEN
        c_num := 0;
        FOR c_id_rec IN c_byname LOOP
        c_num := c_num + 1;
        row_id(c_num) := c_id_rec.rowid;
        END LOOP;
        cust_rowid := row_id ((c_num + 1) / 2);

        SELECT c_balance, c_first, c_middle, c_last, c_id
        INTO os_c_balance, os_c_first, os_c_middle, os_c_last, os_c_id
        FROM customer
        WHERE rowid = cust_rowid;
        ELSE
        SELECT c_balance, c_first, c_middle, c_last, rowid
        INTO os_c_balance, os_c_first, os_c_middle, os_c_last, cust_rowid
        FROM customer
        WHERE c_id = os_c_id AND c_d_id = os_d_id AND c_w_id = os_w_id;
        END IF;

        -- The following statement in the TPC-C specification appendix is incorrect
        -- as it does not include the where clause and does not restrict the
        -- results set giving an ORA-01422.
        -- The statement has been modified in accordance with the
        -- descriptive specification as follows:
        -- The row in the ORDER table with matching O_W_ID (equals C_W_ID),
        -- O_D_ID (equals C_D_ID), O_C_ID (equals C_ID), and with the largest
        -- existing O_ID, is selected. This is the most recent order placed by that
        -- customer. O_ID, O_ENTRY_D, and O_CARRIER_ID are retrieved.
        BEGIN
        SELECT o_id, o_carrier_id, o_entry_d
        INTO os_o_id, os_o_carrier_id, os_entdate
        FROM (SELECT o_id, o_carrier_id, o_entry_d
        FROM OORDER
        WHERE o_d_id = os_d_id AND o_w_id = os_w_id and o_c_id=os_c_id
        ORDER BY o_id DESC)
        WHERE ROWNUM = 1;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
        dbms_output.put_line('No orders for customer');
        END;

        i := 0;
        FOR os_c_line IN c_line
        LOOP
        os_ol_i_id(i) := os_c_line.ol_i_id;
        os_ol_supply_w_id(i) := os_c_line.ol_supply_w_id;
        os_ol_quantity(i) := os_c_line.ol_quantity;
        os_ol_amount(i) := os_c_line.ol_amount;
        os_ol_delivery_d(i) := os_c_line.ol_delivery_d;
        i := i + 1;
        END LOOP;
        COMMIT;

        EXCEPTION WHEN not_serializable OR deadlock OR snapshot_too_old THEN
        ROLLBACK;
    END",
            r"CREATE OR REPLACE PROCEDURE SLEV (
        st_w_id			INTEGER,
        st_d_id			INTEGER,
        threshold		INTEGER,
        stock_count		OUT INTEGER
        )
        IS
        st_o_id			NUMBER;
        not_serializable		EXCEPTION;
        PRAGMA EXCEPTION_INIT(not_serializable,-8177);
        deadlock			EXCEPTION;
        PRAGMA EXCEPTION_INIT(deadlock,-60);
        snapshot_too_old		EXCEPTION;
        PRAGMA EXCEPTION_INIT(snapshot_too_old,-1555);
        BEGIN
        SELECT COUNT(DISTINCT (s_i_id))
        INTO stock_count
        FROM order_line, stock, district
        WHERE d_id=st_d_id
        AND d_w_id=st_w_id
        AND d_id = ol_d_id
        AND d_w_id = ol_w_id
        AND ol_i_id = s_i_id
        AND ol_w_id = s_w_id
        AND s_quantity < threshold
        AND ol_o_id BETWEEN (d_next_o_id - 20) AND (d_next_o_id - 1);

        COMMIT;
        EXCEPTION
        WHEN not_serializable OR deadlock OR snapshot_too_old THEN
        ROLLBACK;
    END",
        ];
        info!("Creating procedures...");
        for sql in sql_set {
            exec.execute(sql).await?;
        }
        info!("Procedures created.");

        Ok(())
    }

    async fn destroy_schema(&self) -> anyhow::Result<()> {
        let conn = self
            .connect(self.connection.connections.schema.to_string())
            .await?;

        // Drop tables
        let tables = [
            "CUSTOMER",
            "DISTRICT",
            "HISTORY",
            "ITEM",
            "WAREHOUSE",
            "STOCK",
            "NEW_ORDER",
            "OORDER",
            "ORDER_LINE",
        ];
        let mut exec = SimpleExecutor::new(StatementHandle::new(&conn.conn_handle)?);
        for table in tables {
            info!(table, "Dropping table");
            match exec.execute(&format!("drop table {table}")).await {
                Ok(_) => info!(table, "Table dropped"),
                Err(e) => warn!(table, ?e, "Failed to drop table"),
            }
        }

        // Drop procedures
        let procedures = ["NEWORD", "OSTAT", "PAYMENT", "DELIVERY", "SLEV"];
        let mut exec = SimpleExecutor::new(StatementHandle::new(&conn.conn_handle)?);
        for procedure in procedures {
            info!(procedure, "Dropping procedure");
            match exec.execute(&format!("drop procedure {procedure}")).await {
                Ok(_) => info!(procedure, "Table dropped"),
                Err(e) => warn!(procedure, ?e, "Failed to drop procedure"),
            }
        }
        Ok(())
    }

    async fn loader(&self) -> anyhow::Result<Box<dyn Loader>> {
        Ok(Box::new(YasdbLoader::new(
            self.connect(self.connection.connections.loader.clone())
                .await?,
        )))
    }
}
