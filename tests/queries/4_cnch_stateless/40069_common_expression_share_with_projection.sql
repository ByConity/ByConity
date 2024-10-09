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


set dialect_type = 'MYSQL';
DROP TABLE IF EXISTS tthenghamdext_full_att_adid_active_hourly_report_v1;
DROP TABLE IF EXISTS tthenghamdext_agent_account;

CREATE TABLE tthenghamdext_full_att_adid_active_hourly_report_v1
(
    `id` Int64 NOT NULL,
    `date` Date NOT NULL DEFAULT '2018-01-01',
    `active` Int32 NOT NULL DEFAULT '0',
    `agent_account_id` Int64 NOT NULL DEFAULT '0' COMMENT '账号id'
)
ENGINE = CnchMergeTree
ORDER BY (id, date)
UNIQUE KEY (id, date);

CREATE TABLE tthenghamdext_agent_account
(
    `id` Int64 NOT NULL,
    `account_type` Int8 NOT NULL DEFAULT '1' COMMENT '账号类型  1新增 2内拉新 3召回 4 地推'
)
ENGINE = CnchMergeTree
ORDER BY tuple(id);

SELECT
    concat('490', '1', aa.id) AS idx_hash,
    sum(aadr.active) AS inner_active
FROM tthenghamdext_full_att_adid_active_hourly_report_v1 AS aadr LEFT JOIN tthenghamdext_agent_account AS aa ON aa.id = aadr.agent_account_id
WHERE aa.account_type = 1
GROUP BY idx_hash
HAVING inner_active > 0
UNION
SELECT
    '11' AS idx,
    111 AS inner;
