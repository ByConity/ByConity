DROP TABLE IF EXISTS window_aggregate_rows;
CREATE TABLE window_aggregate_rows
(
    a UInt64,
    b Float64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO window_aggregate_rows (a, b)
VALUES (0, 0) (1, 4.5) (1, -2) (2, 5) (5, 9) (5, 99) (4, 1) (2, 1) (3, 2.1) (0, -5);
SELECT
  a,
  AVG(b) OVER (PARTITION BY a ROWS UNBOUNDED PRECEDING)
FROM
  window_aggregate_rows
ORDER BY a;
DROP TABLE window_aggregate_rows;
