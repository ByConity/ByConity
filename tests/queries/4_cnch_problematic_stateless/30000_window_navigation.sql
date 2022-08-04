DROP TABLE IF EXISTS test.window_navigation;
CREATE TABLE test.window_navigation
(
    a UInt64,
    b String,
    c Float64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO test.window_navigation
VALUES (0, 'a', 4.2) (0, 'a', 4.1) (1, 'a', -2) (0, 'b', 0) (2, 'c', 9) (1, 'b', -55);
SELECT
  a,
  b,
  c,
  RANK() OVER (PARTITION BY b ORDER BY a, c),
  ROW_NUMBER() OVER (PARTITION by a ORDER BY b, c),
  DENSE_RANK() OVER (PARTITION BY c ORDER BY a, b)
FROM test.window_navigation
ORDER BY a, b, c;
DROP TABLE test.window_navigation;
