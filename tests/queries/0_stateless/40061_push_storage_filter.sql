SET enable_optimizer = 1;

CREATE DATABASE IF NOT EXISTS test;
USE test;

DROP TABLE IF EXISTS t40061 ;
DROP TABLE IF EXISTS t40061_local ;

CREATE TABLE t40061_local (p_date Date, a Int32, b Int32) engine = MergeTree() PARTITION BY p_date ORDER BY a;
CREATE TABLE t40061 (p_date Date, a Int32, b Int32) engine = Distributed(test_shard_localhost, currentDatabase(), 't40061_local');
INSERT INTO t40061_local VALUES ('2022-12-31', 1, 20) ('2023-01-01', 2, 30);

-- { echoOn }
SET enable_partition_filter_push_down = 0;
SET enable_optimizer_early_prewhere_push_down = 0;
explain select a from t40061 where p_date >= '2023-01-01';
explain select a, p_date from t40061 where p_date >= '2023-01-01';
explain select a from t40061 where b > 0 AND p_date >= '2023-01-01';
explain select s.a, t.a from (select a from t40061 where b > 0 AND p_date >= '2023-01-01') s, (select a from t40061 where b > 1 AND p_date >= '2023-01-01') t;
explain select a from t40061 where toYear(p_date) >= 2023;
explain select a from t40061 where toYear(p_date) >= 2023 AND b > 0 AND p_date >= '2023-01-01' AND a + 1 > 0;

SET enable_partition_filter_push_down = 1;
SET enable_optimizer_early_prewhere_push_down = 0;
explain select a from t40061 where p_date >= '2023-01-01';
explain select a, p_date from t40061 where p_date >= '2023-01-01';
explain select a from t40061 where b > 0 AND p_date >= '2023-01-01';
explain select s.a, t.a from (select a from t40061 where b > 0 AND p_date >= '2023-01-01') s, (select a from t40061 where b > 1 AND p_date >= '2023-01-01') t;
explain select a from t40061 where toYear(p_date) >= 2023;
explain select a from t40061 where toYear(p_date) >= 2023 AND b > 0 AND p_date >= '2023-01-01' AND a + 1 > 0;

SET enable_partition_filter_push_down = 1;
SET enable_optimizer_early_prewhere_push_down = 1;
explain select a from t40061 where p_date >= '2023-01-01';
explain select a, p_date from t40061 where p_date >= '2023-01-01';
explain select a from t40061 where b > 0 AND p_date >= '2023-01-01';
explain select s.a, t.a from (select a from t40061 where b > 0 AND p_date >= '2023-01-01') s, (select a from t40061 where b > 1 AND p_date >= '2023-01-01') t;
explain select a from t40061 where toYear(p_date) >= 2023;
explain select a from t40061 where toYear(p_date) >= 2023 AND b > 0 AND p_date >= '2023-01-01' AND a + 1 > 0;

SET enable_optimizer = 0;

SET enable_partition_filter_push_down = 0;
SET enable_early_partition_pruning = 0;
select count() from t40061 where p_date >= '2023-01-01';
explain select count() from t40061 where p_date >= '2023-01-01';

SET enable_partition_filter_push_down = 1;
SET enable_early_partition_pruning = 0;
select count() from t40061 where p_date >= '2023-01-01';
explain select count() from t40061 where p_date >= '2023-01-01';

SET enable_partition_filter_push_down = 1;
SET enable_early_partition_pruning = 1;
select count() from t40061 where p_date >= '2023-01-01';
explain select count() from t40061 where p_date >= '2023-01-01';

-- { echoOff }

DROP TABLE IF EXISTS t40061 ;
DROP TABLE IF EXISTS t40061_local ;