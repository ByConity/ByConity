-- lower case
set dialect_type='MYSQL';
set text_case_option = 'LOWERCASE';
set enable_optimizer=1;
DROP TABLE IF EXISTS test_CASE0;
DROP TABLE IF EXISTS test_CASE1;
DROP TABLE IF EXISTS test_CASE2;

CREATE TABLE test_CASE0 (Xx String) ENGINE = CnchMergeTree() ORDER BY xX AS SELECT 1;
SELECT XX, toTypeName(xx) FROM test_CASE0;

create table test_CASE1 (
    EVENT_date Date,
    EVENT_TYPE String,
    EVENT_COUNT Int32,
    EventDate Date DEFAULT toDate(EVENT_date)
) engine = CnchMergeTree()
partition by toYYYYMM(EVENT_DATE)
order by (EVENT_type, EVENT_count);


CREATE TABLE test_CASE2
(
    ID UInt64,
    updated_AT DateTime64 MATERIALIZED now(),
    SIze_bytes Int64,
    SIZE String Alias formatReadableSize(size_bytes),
    PRIMARY KEY(Id)
)
ENGINE = CnchMergeTree
ORDER BY id;

show create table test_CASE0;
show create table test_CASE1;
show create table test_CASE2;


DROP TABLE test_CASE0;
DROP TABLE test_CASE1;
DROP TABLE test_CASE2;

DROP TABLE IF EXISTS test_cte_CASE1;

create table test_cte_CASE1 (
    EVENT_date Date,
    EVENT_TYPE String,
    EVENT_COUNT Int32,
    EventDate Date DEFAULT toDate(EVENT_date)
) engine = CnchMergeTree()
partition by toYYYYMM(EVENT_DATE)
order by (EVENT_type, EVENT_count);


INSERT INTO test_cte_CASE1 (EVENT_DATE, EVENT_TYPE, EVENT_COUNT) VALUES
('2022-01-01', 'Type1', 10), ('2022-01-02', 'Type2', 20), ('2022-01-03', 'Type3', 30);

-- Test Case 1: Simple Select with CTE
WITH cte_events AS
(
    SELECT EVENT_DATE, EVENT_TYPE
    FROM test_cte_CASE1
)
SELECT * FROM cte_events ORDER BY EVENT_DATE;

-- Test Case 2: CTE with Aggregation
WITH cte_event_counts AS
(
    SELECT EVENT_TYPE, SUM(EVENT_COUNT) AS total_count
    FROM test_cte_CASE1
    GROUP BY EVENT_TYPE
)
SELECT EVENT_TYPE, total_count FROM cte_event_counts ORDER BY total_count;

-- Test Case 3: Nested CTEs
WITH cte_dates AS
(
    SELECT EVENT_DATE
    FROM test_cte_CASE1
),
cte_event_counts AS
(
    SELECT EVENT_DATE, COUNT(*) AS count
    FROM cte_dates
    GROUP BY EVENT_DATE
)
SELECT * FROM cte_event_counts order by event_date;

-- Test Case 4: CTE in WHERE clause
WITH cte_types AS
(
    SELECT EVENT_TYPE
    FROM test_cte_CASE1
    WHERE EVENT_COUNT > 15
)
SELECT * FROM test_cte_CASE1
WHERE EVENT_TYPE IN (SELECT EVENT_TYPE FROM cte_types) order by event_type;
DROP TABLE test_CTE_CASE1;

DrOp DATabASE If existS mYSQl_teST_lower1;
crEaTe dAtaBAsE mYSQl_teST_lower1;
uSe mYSQl_teST_lower1;
crEatE taBlE mYSQl_teST_lower1.tesT_skipIndEx(
`ID` TinYinT,
`KeY_I` inTEgEr,
`p_daTE` daTe,
`str` varchar(10)
)engINE = CnchMergeTree
pARtiTIon by P_date
ORdER by iD
SeTTinGs index_granularity = 8192;

DROP DATABASE mYSQl_teST_lower1;
