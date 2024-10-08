set dialect_type='CLICKHOUSE';
set enable_optimizer = 1;

drop table if exists array_element_rewrite_46017;
CREATE TABLE array_element_rewrite_46017
(
    `id` UInt64,
    `kv` Map(String, UInt64) KV,
    `byte` Map(String, UInt64)
)
ENGINE = CnchMergeTree
ORDER BY id;

EXPLAIN
SELECT ta.kv['456']
FROM array_element_rewrite_46017 AS ta
WHERE (kv['123']) = 1;

EXPLAIN
SELECT ta.byte['456']
FROM array_element_rewrite_46017 AS ta
WHERE (byte['123']) = 1;

EXPLAIN
SELECT ta.kv['456']
FROM array_element_rewrite_46017 AS ta
FULL OUTER JOIN
(
    SELECT *
    FROM array_element_rewrite_46017
    WHERE (kv['123']) = 1
) AS tb ON ((ta.kv['789']) + (tb.kv['789'])) = 0;

EXPLAIN
SELECT ta.byte['456']
FROM array_element_rewrite_46017 AS ta
FULL OUTER JOIN
(
    SELECT *
    FROM array_element_rewrite_46017
    WHERE (byte['123']) = 1
) AS tb ON ((ta.byte['789']) + (tb.byte['789'])) = 0;

-- NOTE: arrayElement is still buggy
-- CLICKHOUSE dialect uses non-optimizer getSampleBlock, 
--    where subquery as table will trigger this bug
-- so we have to use ANSI to test
-- a furture mr will fix this fully. 
set dialect_type='ANSI';
EXPLAIN
SELECT ta.kv['456']
FROM array_element_rewrite_46017 AS ta
FULL OUTER JOIN
(
    SELECT *
    FROM array_element_rewrite_46017
    WHERE (kv['123']) = 1
) AS tb ON ((ta.kv['789']) + (tb.kv['789'])) = 0;

EXPLAIN
SELECT ta.byte['456']
FROM array_element_rewrite_46017 AS ta
FULL OUTER JOIN
(
    SELECT *
    FROM array_element_rewrite_46017
    WHERE (byte['123']) = 1
) AS tb ON ((ta.byte['789']) + (tb.byte['789'])) = 0;

drop table if exists array_element_rewrite_46017;