CREATE
OR REPLACE PROCEDURE PAYMENT (
  p_w_id INTEGER,
  p_d_id INTEGER,
  p_c_w_id INTEGER,
  p_c_d_id INTEGER,
  p_c_id IN OUT INTEGER,
  byname INTEGER,
  p_h_amount NUMBER,
  p_c_last IN OUT VARCHAR2,
  p_w_street_1 OUT VARCHAR2,
  p_w_street_2 OUT VARCHAR2,
  p_w_city OUT VARCHAR2,
  p_w_state OUT VARCHAR2,
  p_w_zip OUT VARCHAR2,
  p_d_street_1 OUT VARCHAR2,
  p_d_street_2 OUT VARCHAR2,
  p_d_city OUT VARCHAR2,
  p_d_state OUT VARCHAR2,
  p_d_zip OUT VARCHAR2,
  p_c_first OUT VARCHAR2,
  p_c_middle OUT VARCHAR2,
  p_c_street_1 OUT VARCHAR2,
  p_c_street_2 OUT VARCHAR2,
  p_c_city OUT VARCHAR2,
  p_c_state OUT VARCHAR2,
  p_c_zip OUT VARCHAR2,
  p_c_phone OUT VARCHAR2,
  p_c_since OUT DATE,
  p_c_credit IN OUT VARCHAR2,
  p_c_credit_lim OUT NUMBER,
  p_c_discount OUT NUMBER,
  p_c_balance IN OUT NUMBER,
  p_c_data OUT VARCHAR2,
  timestamp IN DATE
) IS p_d_name VARCHAR2(11);
p_w_name VARCHAR2(11);
p_c_new_data VARCHAR2(500);
h_data VARCHAR2(30);
TYPE rowidarray IS TABLE OF ROWID INDEX BY BINARY_INTEGER;
cust_rowid ROWID;
row_id rowidarray;
c_num BINARY_INTEGER;
CURSOR c_byname IS
SELECT
  rowid
FROM
  customer
WHERE
  c_w_id = p_c_w_id
  AND c_d_id = p_c_d_id
  AND c_last = p_c_last
ORDER BY
  c_first;
-- TODO yashandb
  -- not_serializable		EXCEPTION;
  --         PRAGMA EXCEPTION_INIT(not_serializable,-8177);
  deadlock
EXCEPTION;
PRAGMA EXCEPTION_INIT(deadlock, 2023);
snapshot_too_old
EXCEPTION;
PRAGMA EXCEPTION_INIT(snapshot_too_old, 2020);
BEGIN
  IF (byname = 1) THEN c_num := 0;
FOR c_id_rec IN c_byname
LOOP
  c_num := c_num + 1;
row_id (c_num) := c_id_rec.rowid;
END
LOOP;
cust_rowid := row_id ((c_num + 1) / 2);
UPDATE
  customer
SET
  c_balance = c_balance - p_h_amount --c_ytd_payment = c_ytd_payment + hist_amount,
  --c_payment_cnt = c_payment_cnt + 1
WHERE
  rowid = cust_rowid;
select
  c_id,
  c_first,
  c_middle,
  c_last,
  c_street_1,
  c_street_2,
  c_city,
  c_state,
  c_zip,
  c_phone,
  c_since,
  c_credit,
  c_credit_lim,
  c_discount,
  c_balance INTO p_c_id,
  p_c_first,
  p_c_middle,
  p_c_last,
  p_c_street_1,
  p_c_street_2,
  p_c_city,
  p_c_state,
  p_c_zip,
  p_c_phone,
  p_c_since,
  p_c_credit,
  p_c_credit_lim,
  p_c_discount,
  p_c_balance
FROM
  customer
WHERE
  rowid = cust_rowid;
  ELSE
UPDATE
  customer
SET
  c_balance = c_balance - p_h_amount --c_ytd_payment = c_ytd_payment + hist_amount,
  --c_payment_cnt = c_payment_cnt + 1
WHERE
  c_id = p_c_id
  AND c_d_id = p_c_d_id
  AND c_w_id = p_c_w_id;
SELECT
  rowid,
  c_first,
  c_middle,
  c_last,
  c_street_1,
  c_street_2,
  c_city,
  c_state,
  c_zip,
  c_phone,
  c_since,
  c_credit,
  c_credit_lim,
  c_discount,
  c_balance INTO cust_rowid,
  p_c_first,
  p_c_middle,
  p_c_last,
  p_c_street_1,
  p_c_street_2,
  p_c_city,
  p_c_state,
  p_c_zip,
  p_c_phone,
  p_c_since,
  p_c_credit,
  p_c_credit_lim,
  p_c_discount,
  p_c_balance
FROM
  customer
WHERE
  c_id = p_c_id
  AND c_d_id = p_c_d_id
  AND c_w_id = p_c_w_id;
END
  IF;
IF p_c_credit = 'BC' THEN
UPDATE
  customer
SET
  c_data = substr(
    (
      to_char(p_c_id) || ' ' || to_char(p_c_d_id) || ' ' || to_char(p_c_w_id) || ' ' || to_char(p_d_id) || ' ' || to_char(p_w_id) || ' ' || to_char(p_h_amount, '9999.99') || ' | '
    ) || c_data,
    1,
    500
  )
WHERE
  rowid = cust_rowid;
select
  substr(c_data, 1, 200) INTO p_c_data
FROM
  CUSTOMER
where
  rowid = cust_rowid;
  ELSE p_c_data := ' ';
END
  IF;
UPDATE
  district
SET
  d_ytd = d_ytd + p_h_amount
WHERE
  d_id = p_d_id
  AND d_w_id = p_w_id;
SELECT
  d_name,
  d_street_1,
  d_street_2,
  d_city,
  d_state,
  d_zip INTO p_d_name,
  p_d_street_1,
  p_d_street_2,
  p_d_city,
  p_d_state,
  p_d_zip
FROM
  district
where
  d_id = p_d_id
  AND d_w_id = p_w_id;
UPDATE
  warehouse
SET
  w_ytd = w_ytd + p_h_amount
WHERE
  w_id = p_w_id;
SELECT
  w_name,
  w_street_1,
  w_street_2,
  w_city,
  w_state,
  w_zip INTO p_w_name,
  p_w_street_1,
  p_w_street_2,
  p_w_city,
  p_w_state,
  p_w_zip
FROM
  warehouse
WHERE
  w_id = p_w_id;
INSERT INTO
  history (
    h_c_id,
    h_c_d_id,
    h_c_w_id,
    h_d_id,
    h_w_id,
    h_date,
    h_amount,
    h_data
  )
VALUES
  (
    p_c_id,
    p_c_d_id,
    p_c_w_id,
    p_d_id,
    p_w_id,
    timestamp,
    p_h_amount,
    p_w_name || ' ' || p_d_name
  );
COMMIT;
EXCEPTION
  -- WHEN not_serializable OR deadlock OR snapshot_too_old
  WHEN deadlock
  OR snapshot_too_old THEN ROLLBACK;
END;
