SET enable_optimizer = 1;
SET enable_optimizer_fallback = 0;

DROP TABLE IF EXISTS t40052_1;
CREATE TABLE t40052_1(a Int32, b Int32) ENGINE = CnchMergeTree() ORDER BY a;
INSERT INTO t40052_1 VALUES (1, 1), (2, 2), (3, 1), (2, 2), (3, 1), (3, 2);
DROP TABLE IF EXISTS t40052_2;
CREATE TABLE t40052_2(a Int32, b Int32) ENGINE = CnchMergeTree() ORDER BY a;
INSERT INTO t40052_2 VALUES (1, 1), (2, 2), (3, 1), (2, 2), (3, 1), (3, 2);

SELECT '-----------simple test------------';
SELECT a, count(*) FROM t40052_1 GROUP BY a WITH TOTALS ORDER BY a;
SELECT a, count(*) FROM t40052_1 GROUP BY a WITH TOTALS HAVING a >= 2 ORDER BY a;
SELECT a, count(*) FROM t40052_1 GROUP BY a WITH TOTALS HAVING count(*) >= 2 ORDER BY a;
SELECT a, sum_metric_1, sum_metric_2 FROM (SELECT a, sum(b) AS sum_metric_1 FROM t40052_1 GROUP BY a WITH TOTALS) AS subquery_1 ALL FULL OUTER JOIN (SELECT a, sum(b) AS sum_metric_2 FROM t40052_2 GROUP BY a WITH TOTALS) AS subquery_2 USING a ORDER BY a ASC;

SELECT a, b, count(*) FROM t40052_1 GROUP BY a, b WITH ROLLUP WITH TOTALS ORDER BY a, b; -- { serverError 48 }

-- FIXME: offloading not keep total block order
SELECT '-----------offloading-----------';
-- SET offloading_with_query_plan = 1;

-- SELECT a, count(*) FROM t40052_1 GROUP BY a WITH TOTALS ORDER BY a;
-- SELECT a, count(*) FROM t40052_1 GROUP BY a WITH TOTALS HAVING a >= 2 ORDER BY a;
-- SELECT a, count(*) FROM t40052_1 GROUP BY a WITH TOTALS HAVING count(*) >= 2 ORDER BY a;

SELECT '-----------offloading and extremes-----------';
-- SET extremes = 1;

-- SELECT a, count(*) FROM t40052_1 GROUP BY a WITH TOTALS ORDER BY a;
-- SELECT a, count(*) FROM t40052_1 GROUP BY a WITH TOTALS HAVING a >= 2 ORDER BY a;
-- SELECT a, count(*) FROM t40052_1 GROUP BY a WITH TOTALS HAVING count(*) >= 2 ORDER BY a;

DROP TABLE IF EXISTS t40052_1;
DROP TABLE IF EXISTS t40052_2;
