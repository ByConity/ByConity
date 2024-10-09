CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS t40047_x;
DROP TABLE IF EXISTS t40047_y;

CREATE TABLE t40047_x(a Int32, b Int32) engine = CnchMergeTree() ORDER BY a;
CREATE TABLE t40047_y(a Int32, c Int32) engine = CnchMergeTree() ORDER BY a;

SET dialect_type = 'CLICKHOUSE';
SET enable_optimizer = 1;

EXPLAIN SELECT (SELECT max(c) FROM t40047_y), (SELECT max(c) FROM t40047_y) + 1 FROM t40047_x AS x;
EXPLAIN SELECT (SELECT max(c) FROM t40047_y AS y1), (SELECT max(y2.c) FROM t40047_y AS y2) + 1 FROM t40047_x AS x;
-- test nested subquery
EXPLAIN SELECT (SELECT max((SELECT max(a) FROM t40047_x)) FROM t40047_y), (SELECT max((SELECT max(a) FROM t40047_x)) FROM t40047_y) + 1 FROM t40047_x AS x;

SET dialect_type = 'ANSI';
SET enable_optimizer = 1;

EXPLAIN SELECT (SELECT max(c) FROM t40047_y), (SELECT max(c) FROM t40047_y) + 1 FROM t40047_x AS x;
EXPLAIN SELECT (SELECT max(c) FROM t40047_y AS y1), (SELECT max(y2.c) FROM t40047_y AS y2) + 1 FROM t40047_x AS x;
-- test correlated subqueries
EXPLAIN SELECT (SELECT max(c) FROM t40047_y AS y1 WHERE c = b), (SELECT max(c) FROM t40047_y AS y2 WHERE c = x.b) + 1 FROM t40047_x AS x;
EXPLAIN SELECT (SELECT max(c) FROM t40047_y AS y1 WHERE c = a), (SELECT max(c) FROM t40047_y AS y2 WHERE c = x.a) + 1 FROM t40047_x AS x;

DROP TABLE IF EXISTS t40047_x;
DROP TABLE IF EXISTS t40047_y;
