DROP TABLE IF EXISTS window_complex_exprs;
CREATE TABLE window_complex_exprs
(
    a UInt64,
    b String,
    c Float64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO window_complex_exprs
VALUES (0, 'a', 4.2) (0, 'a', 4.1) (1, 'a', -2) (0, 'b', 0) (2, 'c', 9) (1, 'b', -55);
SELECT
  a,
  b,
  SUM(c) AS S,
  (RANK() OVER (w1 ORDER BY b)) + SUM(a) AS X,
  (SUM(SUM(c) * 2 + 1) OVER (w1 ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
  + (SUM(SUM(c)) OVER (w1 ORDER BY sipHash64(b) % 2)) AS Y
FROM window_complex_exprs
GROUP BY a, b
WINDOW w1 AS (PARTITION BY a ROWS UNBOUNDED PRECEDING)
ORDER BY a, b;
DROP TABLE window_complex_exprs;
