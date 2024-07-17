drop table if exists test_totals;

create table test_totals (s String, a Int32) engine = CnchMergeTree() order by a;

insert into test_totals select case number % 3 when 0 then 'a' when 1 then 'b' else 'c' end as s, 1 as a  from system.numbers limit 10;

-- totals after union
SELECT *
FROM (
    SELECT
        'a' AS label,
        s,
        sum(1) AS ss
    FROM test_totals
    WHERE s = 'a'
    GROUP BY s
        WITH TOTALS
    UNION ALL
    SELECT
        'b' AS label,
        s,
        sum(1) AS ss
    FROM test_totals
    WHERE s = 'b'
    GROUP BY s
        WITH TOTALS
)
ORDER BY label, s;


-- totals after aggregate
SELECT
    ss,
    arraySort(groupArray(s)) AS arr
FROM
(
    SELECT
        s,
        sum(1) AS ss
    FROM test_totals
    GROUP BY s
        WITH TOTALS
)
GROUP BY ss
ORDER BY ss;

SELECT
    ss,
    arraySort(groupArray(s)) AS arr
FROM
(
    SELECT
        s,
        sum(1) AS ss
    FROM test_totals
    GROUP BY s
        WITH TOTALS
)
GROUP BY ss WITH TOTALS
ORDER BY ss;

-- totals after join
-- SELECT
--     a.s,
--     a.ss,
--     b.s,
--     b.ss
-- FROM
-- (
--     SELECT
--         1 AS key,
--         s,
--         sum(1) AS ss
--     FROM test_totals
--     WHERE s = 'a'
--     GROUP BY s
--         WITH TOTALS
-- ) AS a
-- INNER JOIN
-- (
--     SELECT
--         1 AS key,
--         s,
--         sum(1) AS ss
--     FROM test_totals
--     WHERE s = 'b'
--     GROUP BY s
--         WITH TOTALS
-- ) AS b ON a.key = b.key;
-- ORDER BY a.s, b.s;

drop table if exists test_totals;
