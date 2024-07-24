CREATE
OR REPLACE PROCEDURE OSTAT (
  os_w_id INTEGER,
  os_d_id INTEGER,
  os_c_id IN OUT INTEGER,
  byname INTEGER,
  os_c_last IN OUT VARCHAR2,
  os_c_first OUT VARCHAR2,
  os_c_middle OUT VARCHAR2,
  os_c_balance OUT NUMBER,
  os_o_id OUT INTEGER,
  os_entdate OUT DATE,
  os_o_carrier_id OUT INTEGER
) IS TYPE rowidarray IS TABLE OF ROWID INDEX BY BINARY_INTEGER;
cust_rowid ROWID;
row_id rowidarray;
c_num BINARY_INTEGER;
CURSOR c_byname IS
SELECT
  rowid
FROM
  customer
WHERE
  c_w_id = os_w_id
  AND c_d_id = os_d_id
  AND c_last = os_c_last
ORDER BY
  c_first;
i BINARY_INTEGER;
CURSOR c_line IS
SELECT
  ol_i_id,
  ol_supply_w_id,
  ol_quantity,
  ol_amount,
  ol_delivery_d
FROM
  order_line
WHERE
  ol_o_id = os_o_id
  AND ol_d_id = os_d_id
  AND ol_w_id = os_w_id;
TYPE intarray IS TABLE OF INTEGER index by binary_integer;
os_ol_i_id intarray;
os_ol_supply_w_id intarray;
os_ol_quantity intarray;
TYPE datetable IS TABLE OF DATE INDEX BY BINARY_INTEGER;
os_ol_delivery_d datetable;
TYPE numarray IS TABLE OF NUMBER index by binary_integer;
os_ol_amount numarray;
-- TODO yasdb
  --         not_serializable		EXCEPTION;
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
row_id(c_num) := c_id_rec.rowid;
END
LOOP;
cust_rowid := row_id ((c_num + 1) / 2);
SELECT
  c_balance,
  c_first,
  c_middle,
  c_last,
  c_id INTO os_c_balance,
  os_c_first,
  os_c_middle,
  os_c_last,
  os_c_id
FROM
  customer
WHERE
  rowid = cust_rowid;
  ELSE
SELECT
  c_balance,
  c_first,
  c_middle,
  c_last,
  rowid INTO os_c_balance,
  os_c_first,
  os_c_middle,
  os_c_last,
  cust_rowid
FROM
  customer
WHERE
  c_id = os_c_id
  AND c_d_id = os_d_id
  AND c_w_id = os_w_id;
END
  IF;
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
SELECT
  o_id,
  o_carrier_id,
  o_entry_d INTO os_o_id,
  os_o_carrier_id,
  os_entdate
FROM
  (
    SELECT
      o_id,
      o_carrier_id,
      o_entry_d
    FROM
      OORDER
    WHERE
      o_d_id = os_d_id
      AND o_w_id = os_w_id
      and o_c_id = os_c_id
    ORDER BY
      o_id DESC
  )
WHERE
  ROWNUM = 1;
EXCEPTION
  WHEN NO_DATA_FOUND THEN dbms_output.put_line('No orders for customer');
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
END
LOOP;
COMMIT;
EXCEPTION
  --         WHEN not_serializable OR deadlock OR snapshot_too_old THEN
  WHEN deadlock
  OR snapshot_too_old THEN ROLLBACK;
END;
