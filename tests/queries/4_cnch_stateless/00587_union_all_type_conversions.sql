SET max_threads = 1;
SET enable_optimizer = 0; -- type coercion for union is not supported yet in optimizer.

SELECT 1 UNION ALL SELECT -1;
SELECT x, toTypeName(x) FROM (SELECT 1 AS x UNION ALL SELECT -1);

SELECT 1 UNION ALL SELECT NULL;
SELECT x, toTypeName(x) FROM (SELECT 1 AS x UNION ALL SELECT NULL);

SELECT 1 AS x UNION ALL SELECT NULL UNION ALL SELECT 1.0;
SELECT x, toTypeName(x), count() FROM (SELECT 1 AS x UNION ALL SELECT NULL UNION ALL SELECT 1.0) GROUP BY x;

SELECT arrayJoin(x) AS res FROM (SELECT [1, 2, 3] AS x UNION ALL SELECT [nan, NULL]) ORDER BY res;
