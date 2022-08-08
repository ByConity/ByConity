DROP TABLE IF EXISTS test.window_chaining;
CREATE TABLE test.window_chaining
(
    a UInt64,
    b String,
    c Float64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO test.window_chaining
VALUES (0, 'a', 4.2) (0, 'a', 4.1) (1, 'a', -2) (0, 'b', 0) (2, 'c', 9) (1, 'b', -55);
SELECT
  a,
  b,
  SUM(c) AS S,
  RANK() OVER (w1 ORDER BY b) AS X,
  SUM(SUM(c)) OVER (w1 ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Y
FROM test.window_chaining
GROUP BY a, b
WINDOW w1 AS (PARTITION BY a ROWS UNBOUNDED PRECEDING)
ORDER BY a, b;
DROP TABLE test.window_chaining;
