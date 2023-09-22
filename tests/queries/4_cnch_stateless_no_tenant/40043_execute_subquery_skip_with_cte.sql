SELECT a FROM (WITH cte AS (SELECT number FROM system.numbers LIMIT 10) SELECT 1 as a);
