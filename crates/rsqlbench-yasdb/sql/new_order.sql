CREATE
OR REPLACE PROCEDURE NEWORD (
  no_w_id BINARY_INTEGER,
  no_max_w_id BINARY_INTEGER,
  no_d_id BINARY_INTEGER,
  no_c_id BINARY_INTEGER,
  no_o_ol_cnt BINARY_INTEGER,
  no_rollback BOOLEAN,
  no_c_discount OUT NUMBER,
  no_c_last OUT VARCHAR2,
  no_c_credit OUT VARCHAR2,
  no_d_tax OUT NUMBER,
  no_w_tax OUT NUMBER,
  no_d_next_o_id OUT BINARY_INTEGER,
  timestamp IN DATE
) IS order_amount NUMBER;
no_o_all_local BINARY_INTEGER;
loop_counter BINARY_INTEGER;
-- TODO yashan db????
--             not_serializable		EXCEPTION;
--             PRAGMA EXCEPTION_INIT(not_serializable,-8177);
deadlock
EXCEPTION;
PRAGMA EXCEPTION_INIT(deadlock, 2023);
snapshot_too_old
EXCEPTION;
PRAGMA EXCEPTION_INIT(snapshot_too_old, 2020);
--             integrity_viol			EXCEPTION;
  --             PRAGMA EXCEPTION_INIT(integrity_viol,-1);
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
SELECT
  c_discount,
  c_last,
  c_credit,
  w_tax INTO no_c_discount,
  no_c_last,
  no_c_credit,
  no_w_tax
FROM
  customer,
  warehouse
WHERE
  warehouse.w_id = no_w_id
  AND customer.c_w_id = no_w_id
  AND customer.c_d_id = no_d_id
  AND customer.c_id = no_c_id;
--#2.4.1.5
  no_o_all_local := 1;
FOR loop_counter IN 1..no_o_ol_cnt
LOOP
  o_id_array(loop_counter) := round(DBMS_RANDOM.value(low => 1, high => 100000));
--#2.4.1.5.2
  IF (DBMS_RANDOM.value >= 0.01) THEN w_id_array(loop_counter) := no_w_id;
  ELSE no_o_all_local := 0;
w_id_array (loop_counter) := 1 + mod(
    no_w_id + round(
      DBMS_RANDOM.value(low => 0, high => no_max_w_id -1)
    ),
    no_max_w_id
  );
END
  IF;
--#2.4.1.5.3
  o_quantity_array (loop_counter) := round(DBMS_RANDOM.value(low => 1, high => 10));
-- Take advantage of the fact that I'm looping to populate the array used to record order lines at the end
  ol_line_number_array (loop_counter) := loop_counter;
END
LOOP;
UPDATE
  district
SET
  d_next_o_id = d_next_o_id + 1
WHERE
  d_id = no_d_id
  AND d_w_id = no_w_id;
SELECT
  d_next_o_id - 1,
  d_tax INTO no_d_next_o_id,
  no_d_tax
FROM
  district
WHERE
  d_id = no_d_id
  AND d_w_id = no_w_id;
INSERT INTO
  OORDER (
    o_id,
    o_d_id,
    o_w_id,
    o_c_id,
    o_entry_d,
    o_ol_cnt,
    o_all_local
  )
VALUES
  (
    no_d_next_o_id,
    no_d_id,
    no_w_id,
    no_c_id,
    timestamp,
    no_o_ol_cnt,
    no_o_all_local
  );
INSERT INTO
  NEW_ORDER (no_o_id, no_d_id, no_w_id)
VALUES
  (no_d_next_o_id, no_d_id, no_w_id);
-- The HammerDB implementation doesn't do the check for ORIGINAL (which should be done against i_data and s_data)
  IF no_d_id = 1 THEN FOR i IN 1..no_o_ol_cnt
LOOP
UPDATE
  STOCK
SET
  s_quantity = (
    CASE
    WHEN s_quantity < (o_quantity_array(i) + 10) THEN s_quantity + 91
    ELSE s_quantity
    END
  ) - o_quantity_array(i)
WHERE
  s_i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
SELECT
  s_dist_01,
  s_quantity,
  i_price * o_quantity_array(i) INTO district_info(i),
  s_quantity_array(i),
  amount_array(i)
FROM
  stock_item
WHERE
  i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
END
LOOP;
ELSIF no_d_id = 2 THEN FOR i IN 1..no_o_ol_cnt
LOOP
UPDATE
  STOCK
SET
  s_quantity = (
    CASE
    WHEN s_quantity < (o_quantity_array(i) + 10) THEN s_quantity + 91
    ELSE s_quantity
    END
  ) - o_quantity_array(i)
WHERE
  s_i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
select
  s_dist_02,
  s_quantity,
  i_price * o_quantity_array(i) INTO district_info(i),
  s_quantity_array(i),
  amount_array(i)
from
  STOCK_ITEM
WHERE
  i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
END
LOOP;
ELSIF no_d_id = 3 THEN FOR i IN 1..no_o_ol_cnt
LOOP
UPDATE
  STOCK
SET
  s_quantity = (
    CASE
    WHEN s_quantity < (o_quantity_array(i) + 10) THEN s_quantity + 91
    ELSE s_quantity
    END
  ) - o_quantity_array(i)
WHERE
  s_i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
SELECT
  s_dist_03,
  s_quantity,
  i_price * o_quantity_array(i) INTO district_info(i),
  s_quantity_array(i),
  amount_array(i)
FROM
  stock_item
WHERE
  i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
END
LOOP;
ELSIF no_d_id = 4 THEN FOR i IN 1..no_o_ol_cnt
LOOP
UPDATE
  STOCK
SET
  s_quantity = (
    CASE
    WHEN s_quantity < (o_quantity_array(i) + 10) THEN s_quantity + 91
    ELSE s_quantity
    END
  ) - o_quantity_array(i)
WHERE
  s_i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
SELECT
  s_dist_04,
  s_quantity,
  i_price * o_quantity_array(i) INTO district_info(i),
  s_quantity_array(i),
  amount_array(i)
FROM
  STOCK_ITEM
where
  i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
END
LOOP;
ELSIF no_d_id = 5 THEN FOR i IN 1..no_o_ol_cnt
LOOP
UPDATE
  STOCK
SET
  s_quantity = (
    CASE
    WHEN s_quantity < (o_quantity_array(i) + 10) THEN s_quantity + 91
    ELSE s_quantity
    END
  ) - o_quantity_array(i)
WHERE
  s_i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
SELECT
  s_dist_05,
  s_quantity,
  i_price * o_quantity_array(i) INTO district_info(i),
  s_quantity_array(i),
  amount_array(i)
FROM
  STOCK_ITEM
WHERE
  i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
END
LOOP;
ELSIF no_d_id = 6 THEN FOR i IN 1..no_o_ol_cnt
LOOP
UPDATE
  STOCK
SET
  s_quantity = (
    CASE
    WHEN s_quantity < (o_quantity_array(i) + 10) THEN s_quantity + 91
    ELSE s_quantity
    END
  ) - o_quantity_array(i)
WHERE
  s_i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
select
  s_dist_06,
  s_quantity,
  i_price * o_quantity_array(i) INTO district_info(i),
  s_quantity_array(i),
  amount_array(i)
FROM
  STOCK_ITEM
WHERE
  i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
END
LOOP;
ELSIF no_d_id = 7 THEN FOR i IN 1..no_o_ol_cnt
LOOP
UPDATE
  STOCK
SET
  s_quantity = (
    CASE
    WHEN s_quantity < (o_quantity_array(i) + 10) THEN s_quantity + 91
    ELSE s_quantity
    END
  ) - o_quantity_array(i)
WHERE
  s_i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
select
  s_dist_07,
  s_quantity,
  i_price * o_quantity_array(i) INTO district_info(i),
  s_quantity_array(i),
  amount_array(i)
FROM
  STOCK_ITEM
WHERE
  i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
END
LOOP;
ELSIF no_d_id = 8 THEN FOR i IN 1..no_o_ol_cnt
LOOP
UPDATE
  STOCK
SET
  s_quantity = (
    CASE
    WHEN s_quantity < (o_quantity_array(i) + 10) THEN s_quantity + 91
    ELSE s_quantity
    END
  ) - o_quantity_array(i)
WHERE
  s_i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
select
  s_dist_08,
  s_quantity,
  i_price * o_quantity_array(i) INTO district_info(i),
  s_quantity_array(i),
  amount_array(i)
FROM
  STOCK_ITEM
WHERE
  i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
END
LOOP;
ELSIF no_d_id = 9 THEN FOR i IN 1..no_o_ol_cnt
LOOP
UPDATE
  STOCK
SET
  s_quantity = (
    CASE
    WHEN s_quantity < (o_quantity_array(i) + 10) THEN s_quantity + 91
    ELSE s_quantity
    END
  ) - o_quantity_array(i)
WHERE
  s_i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
SELECT
  s_dist_09,
  s_quantity,
  i_price * o_quantity_array(i) INTO district_info(i),
  s_quantity_array(i),
  amount_array(i)
FROM
  STOCK_ITEM
WHERE
  i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
END
LOOP;
ELSIF no_d_id = 10 THEN FOR i IN 1..no_o_ol_cnt
LOOP
UPDATE
  STOCK
SET
  s_quantity = (
    CASE
    WHEN s_quantity < (o_quantity_array(i) + 10) THEN s_quantity + 91
    ELSE s_quantity
    END
  ) - o_quantity_array(i)
WHERE
  s_i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
select
  s_dist_10,
  s_quantity,
  i_price * o_quantity_array(i) INTO district_info(i),
  s_quantity_array(i),
  amount_array(i)
FROM
  STOCK_ITEM
WHERE
  i_id = o_id_array(i)
  AND s_w_id = w_id_array(i);
END
LOOP;
END
  IF;
-- Oracle return the TAX information to the client, presumably to do the calculation there.  HammerDB doesn't return it at all so I'll just calculate it here and do nothing with it
  order_amount := 0;
FOR loop_counter IN 1..no_o_ol_cnt
LOOP
  order_amount := order_amount + (amount_array(loop_counter));
END
LOOP;
order_amount := order_amount * (1 + no_w_tax + no_d_tax) * (1 - no_c_discount);
FORALL i IN 1..no_o_ol_cnt
INSERT INTO
  order_line (
    ol_o_id,
    ol_d_id,
    ol_w_id,
    ol_number,
    ol_i_id,
    ol_supply_w_id,
    ol_quantity,
    ol_amount,
    ol_dist_info
  )
VALUES
  (
    no_d_next_o_id,
    no_d_id,
    no_w_id,
    ol_line_number_array(i),
    o_id_array(i),
    w_id_array(i),
    o_quantity_array(i),
    amount_array(i),
    district_info(i)
  );
-- Rollback 1% of transactions
  IF no_rollback THEN dbms_output.put_line('Rolling back');
ROLLBACK;
  ELSE COMMIT;
END
  IF;
EXCEPTION
  --             WHEN not_serializable OR deadlock OR snapshot_too_old OR integrity_viol --OR no_data_found
  WHEN deadlock
  OR snapshot_too_old --OR no_data_found
  THEN ROLLBACK;
END;
