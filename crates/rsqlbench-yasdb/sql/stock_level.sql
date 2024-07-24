CREATE
OR REPLACE PROCEDURE SLEV (
  st_w_id INTEGER,
  st_d_id INTEGER,
  threshold INTEGER,
  stock_count OUT INTEGER
) IS st_o_id NUMBER;
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
SELECT
  COUNT(DISTINCT (s_i_id)) INTO stock_count
FROM
  order_line,
  stock,
  district
WHERE
  d_id = st_d_id
  AND d_w_id = st_w_id
  AND d_id = ol_d_id
  AND d_w_id = ol_w_id
  AND ol_i_id = s_i_id
  AND ol_w_id = s_w_id
  AND s_quantity < threshold
  AND ol_o_id BETWEEN (d_next_o_id - 20)
  AND (d_next_o_id - 1);
COMMIT;
EXCEPTION
  --         WHEN not_serializable OR deadlock OR snapshot_too_old THEN
  WHEN deadlock
  OR snapshot_too_old THEN ROLLBACK;
END;
