mod loader;
mod terminal;

use crate::cfg::Connection;
use crate::tpcc::loader::Loader;
use async_trait::async_trait;
use loader::MysqlLoader;
use sqlx::mysql::MySqlConnectOptions;

use sqlx::{ConnectOptions, Executor, MySqlConnection};
use terminal::MysqlTerminal;
use tracing::{info, instrument};

use super::{Sut, Terminal};

pub struct MysqlSut {
    connection: Connection,
}

impl MysqlSut {
    pub fn new(connection: Connection) -> Self {
        Self { connection }
    }

    fn build_options_for_schema(&self) -> MySqlConnectOptions {
        let mut mysql = MySqlConnectOptions::new()
            .username(&self.connection.schema_user.username)
            .password(&self.connection.schema_user.password)
            .database(&self.connection.database)
            .host(&self.connection.host)
            .disable_statement_logging();
        if let Some(p) = self.connection.port {
            mysql = mysql.port(p);
        }
        mysql
    }

    #[instrument(skip(self))]
    async fn create_stored_proc(&self, conn: &mut MySqlConnection) -> anyhow::Result<()> {
        #[rustfmt::skip]
        let sql_set = [

r#"
CREATE PROCEDURE `NEWORD`(
    no_w_id INTEGER,
    no_max_w_id INTEGER,
    no_d_id INTEGER,
    no_c_id INTEGER,
    no_o_ol_cnt INTEGER,
    OUT no_c_discount DECIMAL(4, 4),
    OUT no_c_last VARCHAR(16),
    OUT no_c_credit VARCHAR(2),
    OUT no_d_tax DECIMAL(4, 4),
    OUT no_w_tax DECIMAL(4, 4),
    INOUT no_d_next_o_id INTEGER,
    IN timestamp DATETIME
)
BEGIN
    DECLARE no_ol_supply_w_id INTEGER;
    DECLARE no_ol_i_id INTEGER;
    DECLARE no_ol_quantity INTEGER;
    DECLARE no_o_all_local INTEGER;
    DECLARE o_id INTEGER;
    DECLARE no_i_name VARCHAR(24);
    DECLARE no_i_price DECIMAL(5, 2);
    DECLARE no_i_data VARCHAR(50);
    DECLARE no_s_quantity DECIMAL(6);
    DECLARE no_ol_amount DECIMAL(6, 2);
    DECLARE no_s_dist_01 CHAR(24);
    DECLARE no_s_dist_02 CHAR(24);
    DECLARE no_s_dist_03 CHAR(24);
    DECLARE no_s_dist_04 CHAR(24);
    DECLARE no_s_dist_05 CHAR(24);
    DECLARE no_s_dist_06 CHAR(24);
    DECLARE no_s_dist_07 CHAR(24);
    DECLARE no_s_dist_08 CHAR(24);
    DECLARE no_s_dist_09 CHAR(24);
    DECLARE no_s_dist_10 CHAR(24);
    DECLARE no_ol_dist_info CHAR(24);
    DECLARE no_s_data VARCHAR(50);
    DECLARE x INTEGER;
    DECLARE rbk INTEGER;
    DECLARE loop_counter INT;
    DECLARE `Constraint Violation` CONDITION FOR SQLSTATE '23000';
    DECLARE EXIT HANDLER FOR `Constraint Violation` ROLLBACK;
    DECLARE EXIT HANDLER FOR NOT FOUND ROLLBACK;
    SET no_o_all_local = 1;
    SELECT c_discount, c_last, c_credit, w_tax
    INTO no_c_discount, no_c_last, no_c_credit, no_w_tax
    FROM customer,
         warehouse
    WHERE warehouse.w_id = no_w_id
      AND customer.c_w_id = no_w_id
      AND customer.c_d_id = no_d_id
      AND customer.c_id = no_c_id;
    START TRANSACTION;
    SELECT d_next_o_id, d_tax
    INTO no_d_next_o_id, no_d_tax
    FROM district
    WHERE d_id = no_d_id
      AND d_w_id = no_w_id FOR
    UPDATE;
    UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = no_d_id AND d_w_id = no_w_id;
    SET o_id = no_d_next_o_id;
    SET rbk = FLOOR(1 + (RAND() * 99));
    SET loop_counter = 1;
    WHILE loop_counter <= no_o_ol_cnt
        DO
            IF ((loop_counter = no_o_ol_cnt) AND (rbk = 1))
            THEN
                SET no_ol_i_id = 100001;
            ELSE
                SET no_ol_i_id = FLOOR(1 + (RAND() * 100000));
            END IF;
            SET x = FLOOR(1 + (RAND() * 100));
            IF (x > 1)
            THEN
                SET no_ol_supply_w_id = no_w_id;
            ELSE
                SET no_ol_supply_w_id = no_w_id;
                SET no_o_all_local = 0;
                WHILE ((no_ol_supply_w_id = no_w_id) AND (no_max_w_id != 1))
                    DO
                        SET no_ol_supply_w_id = FLOOR(1 + (RAND() * no_max_w_id));
                    END WHILE;
            END IF;
            SET no_ol_quantity = FLOOR(1 + (RAND() * 10));
            SELECT i_price, i_name, i_data
            INTO no_i_price, no_i_name, no_i_data
            FROM item
            WHERE i_id = no_ol_i_id;
            SELECT s_quantity,
                   s_data,
                   s_dist_01,
                   s_dist_02,
                   s_dist_03,
                   s_dist_04,
                   s_dist_05,
                   s_dist_06,
                   s_dist_07,
                   s_dist_08,
                   s_dist_09,
                   s_dist_10
            INTO no_s_quantity, no_s_data, no_s_dist_01, no_s_dist_02, no_s_dist_03, no_s_dist_04, no_s_dist_05, no_s_dist_06, no_s_dist_07, no_s_dist_08, no_s_dist_09, no_s_dist_10
            FROM stock
            WHERE s_i_id = no_ol_i_id
              AND s_w_id = no_ol_supply_w_id;
            IF (no_s_quantity > no_ol_quantity)
            THEN
                SET no_s_quantity = (no_s_quantity - no_ol_quantity);
            ELSE
                SET no_s_quantity = (no_s_quantity - no_ol_quantity + 91);
            END IF;
            UPDATE stock
            SET s_quantity = no_s_quantity
            WHERE s_i_id = no_ol_i_id
              AND s_w_id = no_ol_supply_w_id;
            SET no_ol_amount = (no_ol_quantity * no_i_price * (1 + no_w_tax + no_d_tax) * (1 - no_c_discount));
            CASE no_d_id
                WHEN 1 THEN SET no_ol_dist_info = no_s_dist_01;
                WHEN 2 THEN SET no_ol_dist_info = no_s_dist_02;
                WHEN 3 THEN SET no_ol_dist_info = no_s_dist_03;
                WHEN 4 THEN SET no_ol_dist_info = no_s_dist_04;
                WHEN 5 THEN SET no_ol_dist_info = no_s_dist_05;
                WHEN 6 THEN SET no_ol_dist_info = no_s_dist_06;
                WHEN 7 THEN SET no_ol_dist_info = no_s_dist_07;
                WHEN 8 THEN SET no_ol_dist_info = no_s_dist_08;
                WHEN 9 THEN SET no_ol_dist_info = no_s_dist_09;
                WHEN 10 THEN SET no_ol_dist_info = no_s_dist_10;
                END CASE;
            INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity,
                                    ol_amount, ol_dist_info)
            VALUES (o_id, no_d_id, no_w_id, loop_counter, no_ol_i_id, no_ol_supply_w_id, no_ol_quantity, no_ol_amount,
                    no_ol_dist_info);
            set loop_counter = loop_counter + 1;
        END WHILE;
    INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
    VALUES (o_id, no_d_id, no_w_id, no_c_id, timestamp, no_o_ol_cnt, no_o_all_local);
    INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (o_id, no_d_id, no_w_id);
    COMMIT;
END
"#,

r#"
CREATE PROCEDURE `DELIVERY`(
    d_w_id INTEGER,
    d_o_carrier_id INTEGER,
    IN timestamp DATETIME
)
BEGIN
    DECLARE d_no_o_id INTEGER;
    DECLARE current_rowid INTEGER;
    DECLARE d_d_id INTEGER;
    DECLARE d_c_id INTEGER;
    DECLARE d_ol_total INTEGER;
    DECLARE deliv_data VARCHAR(100);
    DECLARE loop_counter INT;
    DECLARE `Constraint Violation` CONDITION FOR SQLSTATE '23000';
    DECLARE EXIT HANDLER FOR `Constraint Violation` ROLLBACK;
    SET loop_counter = 1;
    START TRANSACTION;
    WHILE loop_counter <= 10
        DO
            SET d_d_id = loop_counter;
            SELECT no_o_id INTO d_no_o_id FROM new_order WHERE no_w_id = d_w_id AND no_d_id = d_d_id LIMIT 1;
            DELETE FROM new_order WHERE no_w_id = d_w_id AND no_d_id = d_d_id AND no_o_id = d_no_o_id;
            SELECT o_c_id
            INTO d_c_id
            FROM oorder
            WHERE o_id = d_no_o_id
              AND o_d_id = d_d_id
              AND o_w_id = d_w_id;
            UPDATE oorder
            SET o_carrier_id = d_o_carrier_id
            WHERE o_id = d_no_o_id
              AND o_d_id = d_d_id
              AND o_w_id = d_w_id;
            UPDATE order_line
            SET ol_delivery_d = timestamp
            WHERE ol_o_id = d_no_o_id
              AND ol_d_id = d_d_id
              AND ol_w_id = d_w_id;
            SELECT SUM(ol_amount)
            INTO d_ol_total
            FROM order_line
            WHERE ol_o_id = d_no_o_id
              AND ol_d_id = d_d_id
              AND ol_w_id = d_w_id;
            UPDATE customer
            SET c_balance = c_balance + d_ol_total
            WHERE c_id = d_c_id
              AND c_d_id = d_d_id
              AND c_w_id = d_w_id;
            SET deliv_data = CONCAT(d_d_id, ' ', d_no_o_id, ' ', timestamp);
            set loop_counter = loop_counter + 1;
        END WHILE;
    COMMIT;
END
"#,

r#"
CREATE PROCEDURE `PAYMENT`(
    p_w_id INTEGER,
    p_d_id INTEGER,
    p_c_w_id INTEGER,
    p_c_d_id INTEGER,
    INOUT p_c_id INTEGER,
    byname INTEGER,
    p_h_amount DECIMAL(6, 2),
    INOUT p_c_last VARCHAR(16),
    OUT p_w_street_1 VARCHAR(20),
    OUT p_w_street_2 VARCHAR(20),
    OUT p_w_city VARCHAR(20),
    OUT p_w_state CHAR(2),
    OUT p_w_zip CHAR(9),
    OUT p_d_street_1 VARCHAR(20),
    OUT p_d_street_2 VARCHAR(20),
    OUT p_d_city VARCHAR(20),
    OUT p_d_state CHAR(2),
    OUT p_d_zip CHAR(9),
    OUT p_c_first VARCHAR(16),
    OUT p_c_middle CHAR(2),
    OUT p_c_street_1 VARCHAR(20),
    OUT p_c_street_2 VARCHAR(20),
    OUT p_c_city VARCHAR(20),
    OUT p_c_state CHAR(2),
    OUT p_c_zip CHAR(9),
    OUT p_c_phone CHAR(16),
    OUT p_c_since DATETIME,
    INOUT p_c_credit CHAR(2),
    OUT p_c_credit_lim DECIMAL(12, 2),
    OUT p_c_discount DECIMAL(4, 4),
    INOUT p_c_balance DECIMAL(12, 2),
    OUT p_c_data VARCHAR(500),
    IN timestamp DATETIME
)
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE namecnt INTEGER;
    DECLARE p_d_name VARCHAR(11);
    DECLARE p_w_name VARCHAR(11);
    DECLARE p_c_new_data VARCHAR(500);
    DECLARE h_data VARCHAR(30);
    DECLARE loop_counter INT;
    DECLARE `Constraint Violation` CONDITION FOR SQLSTATE '23000';
    DECLARE c_byname CURSOR FOR
        SELECT c_first,
               c_middle,
               c_id,
               c_street_1,
               c_street_2,
               c_city,
               c_state,
               c_zip,
               c_phone,
               c_credit,
               c_credit_lim,
               c_discount,
               c_balance,
               c_since
        FROM customer
        WHERE c_w_id = p_c_w_id
          AND c_d_id = p_c_d_id
          AND c_last = p_c_last
        ORDER BY c_first;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    DECLARE EXIT HANDLER FOR `Constraint Violation` ROLLBACK;
    START TRANSACTION;
    UPDATE warehouse
    SET w_ytd = w_ytd + p_h_amount
    WHERE w_id = p_w_id;
    SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
    INTO p_w_street_1, p_w_street_2, p_w_city, p_w_state, p_w_zip, p_w_name
    FROM warehouse
    WHERE w_id = p_w_id;
    UPDATE district
    SET d_ytd = d_ytd + p_h_amount
    WHERE d_w_id = p_w_id
      AND d_id = p_d_id;
    SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
    INTO p_d_street_1, p_d_street_2, p_d_city, p_d_state, p_d_zip, p_d_name
    FROM district
    WHERE d_w_id = p_w_id
      AND d_id = p_d_id;
    IF (byname = 1)
    THEN
        SELECT count(c_id)
        INTO namecnt
        FROM customer
        WHERE c_last = p_c_last
          AND c_d_id = p_c_d_id
          AND c_w_id = p_c_w_id;
        OPEN c_byname;
        IF (MOD(namecnt, 2) = 1)
        THEN
            SET namecnt = (namecnt + 1);
        END IF;
        SET loop_counter = 0;
        WHILE loop_counter <= (namecnt / 2)
            DO
                FETCH c_byname
                    INTO p_c_first, p_c_middle, p_c_id, p_c_street_1, p_c_street_2, p_c_city,
                        p_c_state, p_c_zip, p_c_phone, p_c_credit, p_c_credit_lim, p_c_discount, p_c_balance, p_c_since;
                set loop_counter = loop_counter + 1;
            END WHILE;
        CLOSE c_byname;
    ELSE
        SELECT c_first,
               c_middle,
               c_last,
               c_street_1,
               c_street_2,
               c_city,
               c_state,
               c_zip,
               c_phone,
               c_credit,
               c_credit_lim,
               c_discount,
               c_balance,
               c_since
        INTO p_c_first, p_c_middle, p_c_last,
            p_c_street_1, p_c_street_2, p_c_city, p_c_state, p_c_zip,
            p_c_phone, p_c_credit, p_c_credit_lim,
            p_c_discount, p_c_balance, p_c_since
        FROM customer
        WHERE c_w_id = p_c_w_id
          AND c_d_id = p_c_d_id
          AND c_id = p_c_id;
    END IF;
    SET p_c_balance = (p_c_balance + p_h_amount);
    IF p_c_credit = 'BC'
    THEN
        SELECT c_data
        INTO p_c_data
        FROM customer
        WHERE c_w_id = p_c_w_id
          AND c_d_id = p_c_d_id
          AND c_id = p_c_id;
        SET h_data = CONCAT(p_w_name, ' ', p_d_name);
        SET p_c_new_data = CONCAT(CAST(p_c_id AS CHAR), ' ', CAST(p_c_d_id AS CHAR), ' ', CAST(p_c_w_id AS CHAR), ' ',
                                  CAST(p_d_id AS CHAR), ' ', CAST(p_w_id AS CHAR), ' ',
                                  CAST(FORMAT(p_h_amount, 2) AS CHAR), CAST(timestamp AS CHAR), h_data);
        SET p_c_new_data = SUBSTR(CONCAT(p_c_new_data, p_c_data), 1, 500 - (LENGTH(p_c_new_data)));
        UPDATE customer
        SET c_balance = p_c_balance,
            c_data    = p_c_new_data
        WHERE c_w_id = p_c_w_id
          AND c_d_id = p_c_d_id
          AND c_id = p_c_id;
    ELSE
        UPDATE customer
        SET c_balance = p_c_balance
        WHERE c_w_id = p_c_w_id
          AND c_d_id = p_c_d_id
          AND c_id = p_c_id;
    END IF;
    SET h_data = CONCAT(p_w_name, ' ', p_d_name);
    INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
    VALUES (p_c_d_id, p_c_w_id, p_c_id, p_d_id, p_w_id, timestamp, p_h_amount, h_data);
    COMMIT;
END
"#,

r#"
CREATE PROCEDURE `OSTAT`(
    os_w_id INTEGER,
    os_d_id INTEGER,
    INOUT os_c_id INTEGER,
    byname INTEGER,
    INOUT os_c_last VARCHAR(16),
    OUT os_c_first VARCHAR(16),
    OUT os_c_middle CHAR(2),
    OUT os_c_balance DECIMAL(12, 2),
    OUT os_o_id INTEGER,
    OUT os_entdate DATETIME,
    OUT os_o_carrier_id INTEGER
)
BEGIN
    DECLARE os_ol_i_id INTEGER;
    DECLARE os_ol_supply_w_id INTEGER;
    DECLARE os_ol_quantity INTEGER;
    DECLARE os_ol_amount INTEGER;
    DECLARE os_ol_delivery_d DATETIME;
    DECLARE done INT DEFAULT 0;
    DECLARE namecnt INTEGER;
    DECLARE i INTEGER;
    DECLARE loop_counter INT;
    DECLARE no_order_status VARCHAR(100);
    DECLARE os_ol_i_id_array VARCHAR(200);
    DECLARE os_ol_supply_w_id_array VARCHAR(200);
    DECLARE os_ol_quantity_array VARCHAR(200);
    DECLARE os_ol_amount_array VARCHAR(200);
    DECLARE os_ol_delivery_d_array VARCHAR(420);
    DECLARE `Constraint Violation` CONDITION FOR SQLSTATE '23000';
    DECLARE c_name CURSOR FOR
        SELECT c_balance, c_first, c_middle, c_id
        FROM customer
        WHERE c_last = os_c_last
          AND c_d_id = os_d_id
          AND c_w_id = os_w_id
        ORDER BY c_first;
    DECLARE c_line CURSOR FOR
        SELECT ol_i_id,
               ol_supply_w_id,
               ol_quantity,
               ol_amount,
               ol_delivery_d
        FROM order_line
        WHERE ol_o_id = os_o_id
          AND ol_d_id = os_d_id
          AND ol_w_id = os_w_id;
    DECLARE EXIT HANDLER FOR `Constraint Violation` ROLLBACK;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    set no_order_status = '';
    set os_ol_i_id_array = 'CSV,';
    set os_ol_supply_w_id_array = 'CSV,';
    set os_ol_quantity_array = 'CSV,';
    set os_ol_amount_array = 'CSV,';
    set os_ol_delivery_d_array = 'CSV,';
    START TRANSACTION;
    IF (byname = 1)
    THEN
        SELECT count(c_id)
        INTO namecnt
        FROM customer
        WHERE c_last = os_c_last
          AND c_d_id = os_d_id
          AND c_w_id = os_w_id;
        IF (MOD(namecnt, 2) = 1)
        THEN
            SET namecnt = (namecnt + 1);
        END IF;
        OPEN c_name;
        SET loop_counter = 0;
        WHILE loop_counter <= (namecnt / 2)
            DO
                FETCH c_name
                    INTO os_c_balance, os_c_first, os_c_middle, os_c_id;
                set loop_counter = loop_counter + 1;
            END WHILE;
        close c_name;
    ELSE
        SELECT c_balance, c_first, c_middle, c_last
        INTO os_c_balance, os_c_first, os_c_middle, os_c_last
        FROM customer
        WHERE c_id = os_c_id
          AND c_d_id = os_d_id
          AND c_w_id = os_w_id;
    END IF;
    set done = 0;
    SELECT o_id, o_carrier_id, o_entry_d
    INTO os_o_id, os_o_carrier_id, os_entdate
    FROM (SELECT o_id, o_carrier_id, o_entry_d
          FROM oorder
          where o_d_id = os_d_id
            AND o_w_id = os_w_id
            and o_c_id = os_c_id
          ORDER BY o_id DESC) AS sb
    LIMIT 1;
    IF done THEN
        set no_order_status = 'No orders for customer';
    END IF;
    set done = 0;
    set i = 0;
    OPEN c_line;
    REPEAT
        FETCH c_line INTO os_ol_i_id, os_ol_supply_w_id, os_ol_quantity, os_ol_amount, os_ol_delivery_d;
        IF NOT done THEN
            set os_ol_i_id_array = CONCAT(os_ol_i_id_array, ',', CAST(i AS CHAR), ',', CAST(os_ol_i_id AS CHAR));
            set os_ol_supply_w_id_array =
                    CONCAT(os_ol_supply_w_id_array, ',', CAST(i AS CHAR), ',', CAST(os_ol_supply_w_id AS CHAR));
            set os_ol_quantity_array =
                    CONCAT(os_ol_quantity_array, ',', CAST(i AS CHAR), ',', CAST(os_ol_quantity AS CHAR));
            set os_ol_amount_array = CONCAT(os_ol_amount_array, ',', CAST(i AS CHAR), ',', CAST(os_ol_amount AS CHAR));
            set os_ol_delivery_d_array =
                    CONCAT(os_ol_delivery_d_array, ',', CAST(i AS CHAR), ',', CAST(os_ol_delivery_d AS CHAR));
            set i = i + 1;
        END IF;
    UNTIL done END REPEAT;
    CLOSE c_line;
    COMMIT;
END
"#,

r#"
CREATE PROCEDURE `SLEV`(
    st_w_id INTEGER,
    st_d_id INTEGER,
    threshold INTEGER,
    OUT stock_count INTEGER
)
BEGIN
    DECLARE st_o_id INTEGER;
    DECLARE `Constraint Violation` CONDITION FOR SQLSTATE '23000';
    DECLARE EXIT HANDLER FOR `Constraint Violation` ROLLBACK;
    DECLARE EXIT HANDLER FOR NOT FOUND ROLLBACK;
    START TRANSACTION;
    SELECT d_next_o_id
    INTO st_o_id
    FROM district
    WHERE d_w_id = st_w_id
      AND d_id = st_d_id;
    SELECT COUNT(DISTINCT (s_i_id))
    INTO stock_count
    FROM order_line,
         stock
    WHERE ol_w_id = st_w_id
      AND ol_d_id = st_d_id
      AND (ol_o_id < st_o_id)
      AND ol_o_id >= (st_o_id - 20)
      AND s_w_id = st_w_id
      AND s_i_id = ol_i_id
      AND s_quantity < threshold;
    COMMIT;
END
"#,
        ];
        info!("Creating stored procs...");
        for sql in sql_set {
            conn.execute(sql).await?;
        }
        info!("Stored procs created.");
        Ok(())
    }
}

#[async_trait]
impl Sut for MysqlSut {
    async fn terminal(&self, _id: u32) -> anyhow::Result<Box<dyn Terminal>> {
        Ok(Box::new(MysqlTerminal::new(
            self.build_options_for_schema().connect().await?,
        )))
    }

    #[instrument(skip(self))]
    async fn build_schema(&self) -> anyhow::Result<()> {
        let mut conn = self.build_options_for_schema().connect().await?;
        #[rustfmt::skip]
        let sql_set = [
"SET FOREIGN_KEY_CHECKS = 0",
r#"CREATE TABLE `warehouse` (
  `w_id` INT(6) NOT NULL,
  `w_ytd` DECIMAL(12, 2) NULL,
  `w_tax` DECIMAL(4, 4) NULL,
  `w_name` VARCHAR(10) BINARY NULL,
  `w_street_1` VARCHAR(20) BINARY NULL,
  `w_street_2` VARCHAR(20) BINARY NULL,
  `w_city` VARCHAR(20) BINARY NULL,
  `w_state` CHAR(2) BINARY NULL,
  `w_zip` CHAR(9) BINARY NULL,
PRIMARY KEY (`w_id`)
)"#,

r#"
CREATE TABLE `district` (
  `d_id` INT(2) NOT NULL,
  `d_w_id` INT(6) NOT NULL,
  `d_ytd` DECIMAL(12, 2) NULL,
  `d_tax` DECIMAL(4, 4) NULL,
  `d_next_o_id` INT NULL,
  `d_name` VARCHAR(10) BINARY NULL,
  `d_street_1` VARCHAR(20) BINARY NULL,
  `d_street_2` VARCHAR(20) BINARY NULL,
  `d_city` VARCHAR(20) BINARY NULL,
  `d_state` CHAR(2) BINARY NULL,
  `d_zip` CHAR(9) BINARY NULL,
PRIMARY KEY (`d_w_id`,`d_id`)
)"#,

r#"
CREATE TABLE `customer` (
  `c_id` INT(5) NOT NULL,
  `c_d_id` INT(2) NOT NULL,
  `c_w_id` INT(6) NOT NULL,
  `c_first` VARCHAR(16) BINARY NULL,
  `c_middle` CHAR(2) BINARY NULL,
  `c_last` VARCHAR(16) BINARY NULL,
  `c_street_1` VARCHAR(20) BINARY NULL,
  `c_street_2` VARCHAR(20) BINARY NULL,
  `c_city` VARCHAR(20) BINARY NULL,
  `c_state` CHAR(2) BINARY NULL,
  `c_zip` CHAR(9) BINARY NULL,
  `c_phone` CHAR(16) BINARY NULL,
  `c_since` DATETIME NULL,
  `c_credit` CHAR(2) BINARY NULL,
  `c_credit_lim` DECIMAL(12, 2) NULL,
  `c_discount` DECIMAL(4, 4) NULL,
  `c_balance` DECIMAL(12, 2) NULL,
  `c_ytd_payment` DECIMAL(12, 2) NULL,
  `c_payment_cnt` INT(8) NULL,
  `c_delivery_cnt` INT(8) NULL,
  `c_data` VARCHAR(500) BINARY NULL,
PRIMARY KEY (`c_w_id`,`c_d_id`,`c_id`),
KEY c_w_id (`c_w_id`,`c_d_id`,`c_last`(16),`c_first`(16))
)"#,

r#"CREATE TABLE `history` (
  `h_c_id` INT NULL,
  `h_c_d_id` INT NULL,
  `h_c_w_id` INT NULL,
  `h_d_id` INT NULL,
  `h_w_id` INT NULL,
  `h_date` DATETIME NULL,
  `h_amount` DECIMAL(6, 2) NULL,
  `h_data` VARCHAR(24) BINARY NULL
)
"#,

r#"
CREATE TABLE `new_order` (
  `no_w_id` INT NOT NULL,
  `no_d_id` INT NOT NULL,
  `no_o_id` INT NOT NULL,
PRIMARY KEY (`no_w_id`, `no_d_id`, `no_o_id`)
)"#,

r#"
CREATE TABLE `oorder` (
  `o_id` INT NOT NULL,
  `o_w_id` INT NOT NULL,
  `o_d_id` INT NOT NULL,
  `o_c_id` INT NULL,
  `o_carrier_id` INT NULL,
  `o_ol_cnt` INT NULL,
  `o_all_local` INT NULL,
  `o_entry_d` DATETIME NULL,
PRIMARY KEY (`o_w_id`,`o_d_id`,`o_id`),
KEY o_w_id (`o_w_id`,`o_d_id`,`o_c_id`,`o_id`)
)"#,

r#"
CREATE TABLE `order_line` (
  `ol_w_id` INT NOT NULL,
  `ol_d_id` INT NOT NULL,
  `ol_o_id` iNT NOT NULL,
  `ol_number` INT NOT NULL,
  `ol_i_id` INT NULL,
  `ol_delivery_d` DATETIME NULL,
  `ol_amount` INT NULL,
  `ol_supply_w_id` INT NULL,
  `ol_quantity` INT NULL,
  `ol_dist_info` CHAR(24) BINARY NULL,
PRIMARY KEY (`ol_w_id`,`ol_d_id`,`ol_o_id`,`ol_number`)
)"#,

r#"
CREATE TABLE `item` (
  `i_id` INT(6) NOT NULL,
  `i_im_id` INT NULL,
  `i_name` VARCHAR(24) BINARY NULL,
  `i_price` DECIMAL(5, 2) NULL,
  `i_data` VARCHAR(50) BINARY NULL,
PRIMARY KEY (`i_id`)
)"#,

r#"
CREATE TABLE `stock` (
  `s_i_id` INT(6) NOT NULL,
  `s_w_id` INT(6) NOT NULL,
  `s_quantity` INT(6) NULL,
  `s_dist_01` CHAR(24) BINARY NULL,
  `s_dist_02` CHAR(24) BINARY NULL,
  `s_dist_03` CHAR(24) BINARY NULL,
  `s_dist_04` CHAR(24) BINARY NULL,
  `s_dist_05` CHAR(24) BINARY NULL,
  `s_dist_06` CHAR(24) BINARY NULL,
  `s_dist_07` CHAR(24) BINARY NULL,
  `s_dist_08` CHAR(24) BINARY NULL,
  `s_dist_09` CHAR(24) BINARY NULL,
  `s_dist_10` CHAR(24) BINARY NULL,
  `s_ytd` BIGINT(10) NULL,
  `s_order_cnt` INT(6) NULL,
  `s_remote_cnt` INT(6) NULL,
  `s_data` VARCHAR(50) BINARY NULL,
PRIMARY KEY (`s_w_id`,`s_i_id`)
)"#,
];
        for sql in sql_set {
            conn.execute(sql).await?;
        }
        Ok(())
    }

    async fn after_loaded(&self) -> anyhow::Result<()> {
        let mut conn = self.build_options_for_schema().connect().await?;
        self.create_stored_proc(&mut conn).await?;
        Ok(())
    }

    async fn destroy_schema(&self) -> anyhow::Result<()> {
        let mut conn = self.build_options_for_schema().connect().await?;
        // Drop order must be promised due to FOREIGN_KEY_CHECKS.
        for table in [
            "new_order",
            "order_line",
            "oorder",
            "history",
            "customer",
            "stock",
            "item",
            "district",
            "warehouse",
        ] {
            info!("Dropping table {table}...");
            let sql = format!("drop table `{table}`;");
            let _ = conn.execute(sql.as_str()).await;
        }
        for proc in ["delivery", "neword", "ostat", "payment", "slev"] {
            info!("Dropping procedure {proc}...");
            let sql = format!("drop procedure `{proc}`;");
            let _ = conn.execute(sql.as_str()).await;
        }
        Ok(())
    }

    async fn loader(&self) -> anyhow::Result<Box<dyn Loader>> {
        Ok(Box::new(MysqlLoader::new(
            self.build_options_for_schema().connect().await?,
        )))
    }
}
