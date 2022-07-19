SET join_use_nulls = 0;
SET enable_optimizer = 0; -- TODO: semi/anti join is not supported by optimizer
SET any_join_distinct_right_table_keys = 1;

SELECT k, a, b
FROM
(
    SELECT nullIf(number, 7) AS k, toString(number) AS a FROM system.numbers LIMIT 10
) js1
ANY INNER JOIN
(
    SELECT number AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) js2 USING (k) ORDER BY k;

SELECT k, a, b
FROM
(
    SELECT number AS k, toString(number) AS a FROM system.numbers LIMIT 10
) js1
ANY LEFT JOIN
(
    SELECT nullIf(number, 8) AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) js2 USING (k) ORDER BY k;

SELECT k, a, b
FROM
(
    SELECT nullIf(number, 7) AS k, toString(number) AS a FROM system.numbers LIMIT 10
) js1
ANY RIGHT JOIN
(
    SELECT nullIf(number, 8) AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) js2 USING (k) ORDER BY k;
