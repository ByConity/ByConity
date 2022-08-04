DROP TABLE IF EXISTS test.wfagg3;
CREATE TABLE test.wfagg3
(
    a UInt64,
    b Float64
)
ENGINE = Memory;

INSERT INTO test.wfagg3 (a, b)
VALUES (0, 0) (1, 4.5) (1, -2) (2, 5) (5, 9) (5, 99) (4, 1) (2, 1) (3, 2.1) (0, -5);
SELECT
  a,
  AVG(b) OVER (PARTITION BY a ROWS UNBOUNDED PRECEDING)
FROM
  test.wfagg3
ORDER BY a;
DROP TABLE test.wfagg3;
