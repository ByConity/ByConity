DROP TABLE IF EXISTS window_frame_expr_preceding;
CREATE TABLE window_frame_expr_preceding
(
    a UInt64,
    b String,
    c Float64,
    d UInt64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO window_frame_expr_preceding
VALUES (0, 'a', 4.2, 1) (0, 'a', 4.1, 1) (1, 'a', -2, 1) (0, 'b', 0, 1) (2, 'c', 9, 3) (1, 'b', -55, 1);
SELECT
  a,
  b,
  SUM(c) AS S,
  RANK() OVER (PARTITION BY a ORDER BY b ROWS d PRECEDING) AS X,
  SUM(SUM(c)) OVER (PARTITION by a ORDER BY b ROWS (d-1) PRECEDING) AS Y
FROM window_frame_expr_preceding
GROUP BY a, b, d
ORDER BY a, b, d;
DROP TABLE window_frame_expr_preceding;
