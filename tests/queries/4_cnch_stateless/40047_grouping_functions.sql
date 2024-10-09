SET enable_optimizer = 1;

DROP TABLE IF EXISTS t40047;
CREATE TABLE t40047 (a Int32, b Nullable(UInt32), c String) ENGINE = CnchMergeTree() ORDER BY a;

INSERT INTO t40047 VALUES (1, 10, 'aa') (2, 20, 'aa') (1, 10, 'aa') (3, 20, 'bb') (2, 10, 'bb') (2, 20, 'bb');

SET enable_push_partial_agg = 0;

SELECT 'test single key';
SELECT GROUPING(a), a, count() FROM t40047 GROUP BY CUBE(a) ORDER BY GROUPING(a), a;
SELECT GROUPING(a), a, count() FROM t40047 GROUP BY ROLLUP(a) ORDER BY GROUPING(a), a;
-- SELECT GROUPING(a), a, count() FROM t40047 GROUP BY GROUPING SETS((a)) ORDER BY GROUPING(a), a;

SELECT 'test multiple keys';
SELECT grouping(a), grouping(b), grouping(c), a, b, c, count() FROM t40047 GROUP BY CUBE(b, a, c) ORDER BY grouping(a), grouping(b), grouping(c), a, b, c;
SELECT grouping(a), grouping(b), grouping(c), a, b, c, count() FROM t40047 GROUP BY ROLLUP(b, a, c) ORDER BY grouping(a), grouping(b), grouping(c), a, b, c;
SELECT grouping(a), grouping(b), grouping(c), a, b, c, count() FROM t40047 GROUP BY GROUPING SETS((a, b), (b, c), (b, a, c)) ORDER BY grouping(a), grouping(b), grouping(c), a, b, c;

SELECT 'test multiple keys 2';
SELECT GROUPING(c), a, b, c, count() FROM t40047 GROUP BY CUBE(b, a, c) ORDER BY grouping(c), a, b, c;
SELECT GROUPING(c), a, b, c, count() FROM t40047 GROUP BY ROLLUP(b, a, c) ORDER BY grouping(c), a, b, c;
SELECT GROUPING(c), a, b, c, count() FROM t40047 GROUP BY GROUPING SETS((a, b), (b, c), (b, a, c)) ORDER BY grouping(c), a, b, c;

SELECT 'test grouping sets with null set';
SELECT GROUPING(c), a, b, c, count() FROM t40047 GROUP BY GROUPING SETS((a, b), (b, c), (b, a, c), ()) ORDER BY GROUPING(c), a, b, c;

SET enable_push_partial_agg = 1;

SELECT 'test single key';
SELECT GROUPING(a), a, count() FROM t40047 GROUP BY CUBE(a) ORDER BY GROUPING(a), a;
SELECT GROUPING(a), a, count() FROM t40047 GROUP BY ROLLUP(a) ORDER BY GROUPING(a), a;
-- SELECT GROUPING(a), a, count() FROM t40047 GROUP BY GROUPING SETS((a)) ORDER BY GROUPING(a), a;

SELECT 'test multiple keys';
SELECT grouping(a), grouping(b), grouping(c), a, b, c, count() FROM t40047 GROUP BY CUBE(b, a, c) ORDER BY grouping(a), grouping(b), grouping(c), a, b, c;
SELECT grouping(a), grouping(b), grouping(c), a, b, c, count() FROM t40047 GROUP BY ROLLUP(b, a, c) ORDER BY grouping(a), grouping(b), grouping(c), a, b, c;
SELECT grouping(a), grouping(b), grouping(c), a, b, c, count() FROM t40047 GROUP BY GROUPING SETS((a, b), (b, c), (b, a, c)) ORDER BY grouping(a), grouping(b), grouping(c), a, b, c;

SELECT 'test multiple keys 2';
SELECT GROUPING(c), a, b, c, count() FROM t40047 GROUP BY CUBE(b, a, c) ORDER BY grouping(c), a, b, c;
SELECT GROUPING(c), a, b, c, count() FROM t40047 GROUP BY ROLLUP(b, a, c) ORDER BY grouping(c), a, b, c;
SELECT GROUPING(c), a, b, c, count() FROM t40047 GROUP BY GROUPING SETS((a, b), (b, c), (b, a, c)) ORDER BY grouping(c), a, b, c;

SELECT 'test grouping sets with null set';
SELECT GROUPING(c), a, b, c, count() FROM t40047 GROUP BY GROUPING SETS((a, b), (b, c), (b, a, c), ()) ORDER BY GROUPING(c), a, b, c;

-- test duplicate expressions & alias
SELECT GROUPING(a), GROUPING(a), GROUPING(a) + 1, a, count() FROM t40047 GROUP BY CUBE(a) ORDER BY GROUPING(a), a;
SELECT GROUPING(a) AS grouping_a, a, count() FROM t40047 GROUP BY CUBE(a) ORDER BY grouping_a, a;

DROP TABLE IF EXISTS t40047;
