set dialect_type='CLICKHOUSE';
set enable_optimizer=1;
SELECT a as x FROM (select 1 as a) WHERE x = 1;
SELECT (1 + 1 > 2) as a WHERE a;
SELECT a as x, sum(b) as y FROM (select 1 as a, 2 as b) GROUP BY x HAVING y > 1;
SELECT a as x, x + 1 as y FROM (select 1 as a);
SELECT a + 1 as a FROM (select 1 as a) WHERE a > 1;
SELECT
    x as y
FROM
(
    SELECT a as x
    FROM
        (SELECT 1 as a)
    WHERE x = 1
)
WHERE y = (SELECT a as z FROM (SELECT 1 as a) WHERE z = 1);


set dialect_type='ANSI';
set enable_optimizer=1;
set enable_optimizer_fallback=0;
SELECT a as x FROM (select 1 as a) WHERE x = 1;
SELECT (1 + 1 > 2) as a WHERE a;
SELECT a as x, sum(b) as y FROM (select 1 as a, 2 as b) GROUP BY x HAVING y > 1;
SELECT a as x, x + 1 as y FROM (select 1 as a);
SELECT a + 1 as a FROM (select 1 as a) WHERE a > 1;
SELECT
    x as y
FROM
    (
        SELECT a as x
        FROM
            (SELECT 1 as a)
        WHERE x = 1
    )
WHERE y = (SELECT a as z FROM (SELECT 1 as a) WHERE z = 1);
