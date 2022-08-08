DROP TABLE IF EXISTS test.window_frame_unbounded_preceding;
CREATE TABLE test.window_frame_unbounded_preceding
(
    a UInt64,
    b String,
    c Float64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO test.window_frame_unbounded_preceding
VALUES (0, 'a', 4.2) (0, 'a', 4.1) (1, 'a', -2) (0, 'b', 0) (2, 'c', 9) (1, 'b', -55);
SELECT
  a,
  b,
  SUM(c) AS S,
  RANK() OVER (PARTITION BY a ORDER BY b ROWS UNBOUNDED PRECEDING) AS X,
  SUM(SUM(c)) OVER (PARTITION by a ORDER BY b ROWS UNBOUNDED PRECEDING) AS Y
FROM test.window_frame_unbounded_preceding
GROUP BY a, b
ORDER BY a, b;
DROP TABLE test.window_frame_unbounded_preceding;
