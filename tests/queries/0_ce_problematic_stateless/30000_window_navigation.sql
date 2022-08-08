DROP TABLE IF EXISTS test.wfnav12;
CREATE TABLE test.wfnav12
(
    a UInt64,
    b String,
    c Float64
)
ENGINE = Memory;

INSERT INTO test.wfnav12
VALUES (0, 'a', 4.2) (0, 'a', 4.1) (1, 'a', -2) (0, 'b', 0) (2, 'c', 9) (1, 'b', -55);
SELECT
  a,
  b,
  c,
  rank() OVER (PARTITION BY b ORDER BY a, c),
  row_number() OVER (PARTITION by a ORDER BY b, c),
  dense_rank() OVER (PARTITION BY c ORDER BY a, b)
FROM test.wfnav12
ORDER BY a, b, c;
DROP TABLE test.wfnav12;
