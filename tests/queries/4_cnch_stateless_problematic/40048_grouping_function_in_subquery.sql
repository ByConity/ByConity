SET enable_optimizer = 1;

SELECT a, b, ga, gb, res FROM (SELECT a, b, grouping(a) AS ga, grouping(b) AS gb, count() AS res FROM (SELECT 1 AS a, 2 AS b) GROUP BY ROLLUP(a, b)) ORDER BY a, b, ga, gb;

WITH cte AS (SELECT a, b, GROUPING(a) AS ga, GROUPING(b) AS gb, count() AS res FROM (SELECT 1 AS a, 2 AS b) GROUP BY GROUPING SETS((a, b), ()))
SELECT a, b, ga, gb, res FROM cte ORDER BY a, b, ga, gb;
