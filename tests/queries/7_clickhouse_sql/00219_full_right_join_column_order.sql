SET enable_shuffle_with_order = 1;
SET any_join_distinct_right_table_keys = 1;

SELECT a, b FROM (SELECT 1 AS a, 2000 AS b) ANY RIGHT JOIN (SELECT 2 AS a, 3000 AS b) USING a, b;
SELECT a, b FROM (SELECT 1 AS a, 2000 AS b) ANY RIGHT JOIN (SELECT 2 AS a, 3000 AS b) USING b, a;

SELECT a, b FROM (SELECT 1 AS a, 2000 AS b) ANY RIGHT JOIN (SELECT 2 AS a, 3000 AS b UNION ALL SELECT 1 AS a, 2000 AS b) USING a, b order by a;
SELECT a, b FROM (SELECT 1 AS a, 2000 AS b) ANY RIGHT JOIN (SELECT 2 AS a, 3000 AS b UNION ALL SELECT 1 AS a, 2000 AS b) USING b, a order by a;

