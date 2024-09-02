create database if not exists test engine = Cnch;
use test;
SET enable_optimizer=1;
SET optimizer_projection_support=1;
SET max_threads=8;
SET exchange_source_pipeline_threads=1;
set enable_common_expression_sharing=1;
set enable_common_expression_sharing_for_prewhere=1;

DROP TABLE IF EXISTS t40069_ces;

CREATE TABLE t40069_ces
(
    `key` Int32,
    `val` Int64
)
ENGINE = CnchMergeTree
ORDER BY tuple();

INSERT INTO t40069_ces
SELECT
    number AS key,
    1 AS val
FROM system.numbers LIMIT 100;

ALTER TABLE t40069_ces ADD PROJECTION proj1
(
    SELECT
        multiIf((((key + 1) + 2) + 3) % 10 = 1, 'a', 'b') as x,
        multiIf((((key + 1) + 2) + 3) % 10 = 2, 'c', 'd') as y,
        sum(val)
    GROUP BY x, y
);

INSERT INTO t40069_ces
SELECT
    number AS key,
    1 AS val
FROM system.numbers LIMIT 100;

EXPLAIN
SELECT sum(val)
FROM t40069_ces
PREWHERE (multiIf((((key + 1) + 2) + 3) % 10 = 1, 'a', 'b') as x) != 'xx'
WHERE (multiIf((((key + 1) + 2) + 3) % 10 = 2, 'c', 'd') as y) != 'xx';

-- EXPLAIN PIPELINE
-- SELECT sum(val)
-- FROM t40069_ces
-- PREWHERE (multiIf((((key + 1) + 2) + 3) % 10 = 1, 'a', 'b') as x) != 'xx'
-- WHERE (multiIf((((key + 1) + 2) + 3) % 10 = 2, 'c', 'd') as y) != 'xx';

SELECT sum(val)
FROM t40069_ces
PREWHERE (multiIf((((key + 1) + 2) + 3) % 10 = 1, 'a', 'b') as x) != 'xx'
WHERE (multiIf((((key + 1) + 2) + 3) % 10 = 2, 'c', 'd') as y) != 'xx';
