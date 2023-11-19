SET enable_optimizer = 1;

DROP TABLE IF EXISTS t40063 ;

CREATE TABLE t40063 (p_date Date, a Int32, b Int32) engine = CnchMergeTree() PARTITION BY p_date ORDER BY a;
INSERT INTO t40063 VALUES ('2022-12-31', 1, 20) ('2023-01-01', 2, 30);

EXPLAIN
select a from t40063 prewhere b > 0 where p_date >= '2023-01-01'
SETTINGS enable_partition_filter_push_down = 1;
