SET enable_optimizer=1;

DROP TABLE IF EXISTS t40088_1;
DROP TABLE IF EXISTS t40088_2;

CREATE TABLE t40088_1(`a` Int32) ENGINE = CnchMergeTree ORDER BY a;
CREATE TABLE t40088_2(`x` Int32) ENGINE = CnchMergeTree ORDER BY x;

INSERT INTO t40088_1 SELECT number FROM system.numbers LIMIT 1000;
INSERT INTO t40088_2 SELECT number FROM system.numbers LIMIT 10;

CREATE STATS t40088_1 FORMAT Null;
CREATE STATS t40088_2 FORMAT Null;

EXPLAIN stats = 0
SELECT count(*)
FROM
(
    SELECT toInt32(((a + 1) + 1) + 1) AS k
    FROM t40088_1
    WHERE k > 1
) AS r
INNER JOIN t40088_2 AS s ON r.k = s.x
SETTINGS
  enable_common_expression_sharing = 1,
  common_expression_sharing_threshold = 1,
  enable_runtime_filter = 1,
  runtime_filter_min_filter_rows = 1,
  runtime_filter_min_filter_factor = 0.001,
  enum_replicate = 0;

SELECT count(*)
FROM
(
    SELECT toInt32(((a + 1) + 1) + 1) AS k
    FROM t40088_1
    WHERE k > 1
) AS r
INNER JOIN t40088_2 AS s ON r.k = s.x
SETTINGS
  enable_common_expression_sharing = 1,
  common_expression_sharing_threshold = 1,
  enable_runtime_filter = 1,
  runtime_filter_min_filter_rows = 1,
  runtime_filter_min_filter_factor = 0.001,
  enum_replicate = 0;

DROP TABLE IF EXISTS t40088_1;
DROP TABLE IF EXISTS t40088_2;
