CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.wfnav2;
CREATE TABLE test.wfnav2 (a UInt64, b String, c Float64) ENGINE = Memory;

INSERT INTO test.wfnav2 VALUES (0, 'a', 4.2) (0, 'a', 4.1) (1, 'a', -2) (0, 'b', 0) (2, 'c', 9) (1, 'b', -55);

SELECT
  a,
  b,
  c,
  percent_rank() OVER (PARTITION BY b ORDER BY a, c ROWS UNBOUNDED PRECEDING),
  cume_dist() OVER (PARTITION by a ORDER BY b, c ROWS UNBOUNDED PRECEDING),
  ntile(2) OVER (PARTITION BY c ORDER BY a, b ROWS UNBOUNDED PRECEDING)
FROM test.wfnav2
ORDER BY a, b, c;

DROP TABLE test.wfnav2;
