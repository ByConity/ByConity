SET join_use_nulls = 1;
SELECT number FROM system.numbers SEMI LEFT JOIN (SELECT number, ['test'] FROM numbers(1)) js2 USING (number) LIMIT 1;
SELECT number FROM system.numbers ANY LEFT  JOIN (SELECT number, ['test'] FROM numbers(1)) js2 USING (number) LIMIT 1;
