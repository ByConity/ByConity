DROP TABLE IF EXISTS test.window_aggregate;
CREATE TABLE test.window_aggregate
(
    a UInt64,
    b Float64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO test.window_aggregate (a, b)
VALUES (0, 0) (1, 4.5) (1, -2) (2, 5) (5, 9) (5, 99) (4, 1) (2, 1) (3, 2.1) (0, -5);
SELECT
  a,
  AVG(b) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
FROM
  test.window_aggregate
ORDER BY a;
DROP TABLE test.window_aggregate;
