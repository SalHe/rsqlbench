CREATE
OR REPLACE PROCEDURE DELIVERY (
  d_w_id INTEGER,
  d_o_carrier_id INTEGER,
  timestamp DATE
) IS TYPE intarray IS TABLE OF INTEGER index by binary_integer;
dist_id_in_array intarray;
dist_id_array intarray;
o_id_array intarray;
order_c_id intarray;
sums intarray;
ordcnt INTEGER;
-- TODO yasdb
--             not_serializable		EXCEPTION;
--             PRAGMA EXCEPTION_INIT(not_serializable,-8177);
deadlock
EXCEPTION;
PRAGMA EXCEPTION_INIT(deadlock, 2023);
snapshot_too_old
EXCEPTION;
PRAGMA EXCEPTION_INIT(snapshot_too_old, 2020);
BEGIN
  FOR i in 1..10
LOOP
  dist_id_in_array(i) := i;
END
LOOP;
FOR d IN 1..10
LOOP
  -- TODO reuse sub-query
SELECT
  no_d_id,
  no_o_id INTO dist_id_array(d),
  o_id_array(d)
FROM
  NEW_ORDER
WHERE
  no_d_id = dist_id_in_array(d)
  AND no_w_id = d_w_id
  AND no_o_id = (
    select
      min (no_o_id)
    from
      new_order
    where
      no_d_id = dist_id_in_array(d)
      and no_w_id = d_w_id
  );
DELETE FROM
  new_order
WHERE
  no_d_id = dist_id_in_array(d)
  AND no_w_id = d_w_id
  AND no_o_id = (
    select
      min(no_o_id)
    from
      new_order
    where
      no_d_id = dist_id_in_array(d)
      and no_w_id = d_w_id
  );
END
LOOP;
ordcnt := SQL % ROWCOUNT;
FOR o in 1..ordcnt
LOOP
UPDATE
  OORDER
SET
  o_carrier_id = d_o_carrier_id
WHERE
  o_id = o_id_array(o)
  AND o_d_id = dist_id_array(o)
  AND o_w_id = d_w_id;
select
  o_c_id INTO order_c_id(o)
from
  OORDER
where
  o_id = o_id_array(o)
  AND o_d_id = dist_id_array(o)
  AND o_w_id = d_w_id;
END
LOOP;
FOR o in 1..ordcnt
LOOP
UPDATE
  order_line
SET
  ol_delivery_d = timestamp
WHERE
  ol_w_id = d_w_id
  AND ol_d_id = dist_id_array(o)
  AND ol_o_id = o_id_array (o);
SELECT
  sum(ol_amount) INTO sums(o)
FROM
  order_line
WHERE
  ol_w_id = d_w_id
  AND ol_d_id = dist_id_array(o)
  AND ol_o_id = o_id_array (o);
END
LOOP;
FORALL c IN 1..ordcnt
UPDATE
  customer
SET
  c_balance = c_balance + sums(c) -- Added this in for the refactor but it's not in the original (although it should be) so I've removed it, to be true to the original
  --, c_delivery_cnt = c_delivery_cnt + 1
WHERE
  c_w_id = d_w_id
  AND c_d_id = dist_id_array(c)
  AND c_id = order_c_id(c);
COMMIT;
EXCEPTION
  --             WHEN not_serializable OR deadlock OR snapshot_too_old THEN
  WHEN deadlock
  OR snapshot_too_old THEN ROLLBACK;
END;
