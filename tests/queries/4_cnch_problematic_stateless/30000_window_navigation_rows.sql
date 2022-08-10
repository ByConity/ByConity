DROP TABLE IF EXISTS test.window_navigation_rows;
CREATE TABLE test.window_navigation_rows
(
    a UInt64,
    b String,
    c Float64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO test.window_navigation_rows
VALUES (0, 'a', 4.2) (0, 'a', 4.1) (1, 'a', -2) (0, 'b', 0) (2, 'c', 9) (1, 'b', -55);
SELECT
  a,
  b,
  c,
  RANK() OVER (PARTITION BY b ORDER BY a, c ROWS UNBOUNDED PRECEDING),
  ROW_NUMBER() OVER (PARTITION by a ORDER BY b, c ROWS UNBOUNDED PRECEDING),
  DENSE_RANK() OVER (PARTITION BY c ORDER BY a, b ROWS UNBOUNDED PRECEDING),
  LEAD(c*a+1, 1, 1) OVER (PARTITION BY b ORDER BY a, c),
  LAG(c*a+1, 1, 1) OVER (PARTITION BY b ORDER BY a, c)
FROM test.window_navigation_rows
ORDER BY a, b, c;

DROP TABLE test.window_navigation_rows;
