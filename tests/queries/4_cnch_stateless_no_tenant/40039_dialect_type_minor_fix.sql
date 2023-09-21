set enable_optimizer = 1;

WITH cte AS (SELECT 1 as col) SELECT cte.col FROM cte;
SELECT EXISTS (SELECT 1);