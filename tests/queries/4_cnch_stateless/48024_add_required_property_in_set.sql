DROP TABLE IF EXISTS test48024;
set enable_optimizer=1;
set enable_sharding_optimize=1;

create table test48024 (
    l_orderkey  Int32,
    l_partkey  Nullable(Int32),
    l_suppkey  Nullable(Int32)
) ENGINE = CnchMergeTree() 
ORDER BY l_orderkey;

explain 
SELECT countDistinct(l_partkey) AS a
FROM test48024
GROUP BY l_suppkey;

EXPLAIN
SELECT *
FROM
(
    SELECT 10 AS a
) AS t1
INNER JOIN
(
    SELECT countDistinct(l_partkey) AS a
    FROM test48024
    GROUP BY l_suppkey
) AS t2 ON t1.a = t2.a;

set enable_merge_require_property=1;

explain 
SELECT countDistinct(l_partkey) AS a
FROM test48024
GROUP BY l_suppkey;

EXPLAIN
SELECT *
FROM
(
    SELECT 10 AS a
) AS t1
INNER JOIN
(
    SELECT countDistinct(l_partkey) AS a
    FROM test48024
    GROUP BY l_suppkey
) AS t2 ON t1.a = t2.a;

EXPLAIN
SELECT *
FROM
(
    SELECT 10 AS a
) AS t1
INNER JOIN
(
    SELECT countDistinct(l_orderkey) AS a
    FROM test48024
    GROUP BY l_suppkey
) AS t2 ON t1.a = t2.a;

DROP TABLE IF EXISTS test48024;
