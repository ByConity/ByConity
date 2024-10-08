set dialect_type='MYSQL';
set enable_optimizer=1;
set text_case_option = 'LOWERCASE';
DROP TABLE IF EXISTS test_SELECT_CASE1;
DROP TABLE IF EXISTS test_SELECT_CASE2;

create table test_SELECT_CASE1 (
    EVENT_date Date,
    EVENT_TYPE String,
    EVENT_COUNT UInt64,
    EventDate Date DEFAULT toDate(EVENT_date)
) engine = CnchMergeTree()
partition by toYYYYMM(EVENT_DATE)
order by (EVENT_type, EVENT_count);

CREATE TABLE test_SELECT_CASE2
(
    ID UInt64,
    updated_AT DateTime,
    updated_at_date Date DEFAULT toDate(updated_AT),
    updated_at_date1 Date MATERIALIZED toDate(updated_at),
    size_bytes UInt64,
    SIZE String Alias formatReadableSize(size_bytes),
    PRIMARY KEY(Id)
)
ENGINE = CnchMergeTree
ORDER BY id;


INSERT INTO test_SELECT_CASE1 (EVENT_date, EVENT_TYPE, EVENT_COUNT) VALUES
('2022-01-01', 'Type1', 100),('2022-01-02', 'Type2', 200),('2022-01-03', 'Type3', 300),('2022-01-04', 'Type4', 400),('2022-01-05', 'Type5', 500),('2022-01-06', 'Type6', 600),('2022-01-07', 'Type7', 700),('2022-01-08', 'Type8', 800),('2022-01-09', 'Type9', 900),('2022-01-10', 'Type10', 1000);


INSERT INTO test_SELECT_CASE2 (ID, size_bytes) VALUES
(1, 1000),(2, 2000),(3, 3000),(4, 4000),(5, 5000),(6, 6000),(7, 7000),(8, 8000),(9, 9000),(10, 10000);

SELECT event_date, EVENT_TYPE, event_count FROM test_SELECT_CASE1 order by EVENT_COUNT;

SELECT toYYYYMM(EVENT_date) AS year_month, event_type FROM test_SELECT_CASE1 ORDER BY year_month;

SELECT t1.event_type, t2.size
FROM test_SELECT_CASE1 AS t1
JOIN test_SELECT_CASE2 AS t2 ON t1.EVENT_count = t2.id;

SELECT * FROM (SELECT event_date, event_type, EVENT_COUNT FROM test_SELECT_CASE1) AS sub WHERE sub.event_type = 'Type1' order by event_count;

SELECT EVENT_type, SUM(event_count) FROM test_SELECT_CASE1 GROUP BY EVENT_type order by SUM(EVENT_COUNT);

SELECT * FROM test_SELECT_CASE2 WHERE size_bytes > 1000 ORDER BY id;

SELECT id, SIZE FROM test_SELECT_CASE2 WHERE size_bytes < 5000 ORDER BY id;

SELECT EVENT_date, event_type FROM test_SELECT_CASE1 ORDER BY EVENT_count DESC;

SELECT toDate(updated_at) AS updated_date, size_bytes, id FROM test_SELECT_CASE2 ORDER BY id;

SELECT t1.EVENT_type, t2.SIZE, t1.event_count
FROM test_SELECT_CASE1 AS t1
INNER JOIN test_SELECT_CASE2 AS t2 ON t1.EVENT_date = t2.updated_at_date
WHERE t1.EVENT_count > 10
ORDER BY t2.size_bytes;

-- Case Insensitive IN Clause
SELECT *
FROM test_SELECT_CASE1
WHERE lower(EVENT_TYPE) IN ('type1', 'type2', 'type3')
ORDER BY EVENT_COUNT;

-- Case Insensitive Aggregation
SELECT lower(EVENT_type) as event_type_lower, SUM(event_count)
FROM test_SELECT_CASE1
GROUP BY event_type_lower
ORDER BY SUM(EVENT_COUNT);

-- Window Function (Ranking Event Types by Count)
SELECT
    EVENT_TYPE,
    EVENT_COUNT,
    rank() OVER (PARTITION BY EVENT_TYPE ORDER BY EVENT_COUNT DESC) as rank
FROM test_SELECT_CASE1 ORDER BY EVENT_COUNT;

-- Combining Case Insensitivity with Window Functions
SELECT
    lower(EVENT_TYPE) as event_type_lower,
    EVENT_COUNT,
    rank() OVER (PARTITION BY lower(EVENT_TYPE) ORDER BY EVENT_COUNT DESC) as rank
FROM test_SELECT_CASE1 ORDER BY EVENT_COUNT;

-- Combining Case Insensitivity with JOINs
SELECT
    lower(t1.EVENT_type) as event_type_lower,
    EVENT_COUNT,
    t2.size
FROM
    test_SELECT_CASE1 AS t1
    JOIN test_SELECT_CASE2 AS t2 ON lower(t1.EVENT_type) = lower(t2.size)
    ORDER BY EVENT_COUNT;

-- Using Case Insensitivity in Subqueries
SELECT *
FROM (SELECT event_date, lower(event_type) as event_type_lower, EVENT_COUNT FROM test_SELECT_CASE1) AS sub
WHERE sub.event_type_lower = 'type1'
ORDER BY event_count;


DROP TABLE test_SELECT_CASE1;
DROP TABLE test_SELECT_CASE2;
