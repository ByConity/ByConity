DROP database if exists test_distinct;
create database test_distinct;

use test_distinct;
DROP TABLE IF EXISTS test_distinct.unique_1;
DROP TABLE IF EXISTS test_distinct.unique_2;

CREATE TABLE test_distinct.unique_1 (event_time DateTime, id UInt64, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(m1) PARTITION BY toDate(event_time) ORDER BY id UNIQUE KEY id;
CREATE TABLE test_distinct.unique_2 (event_time DateTime, id UInt64, id2 UInt32, m2 UInt64) ENGINE = CnchMergeTree(id2) PARTITION BY toDate(event_time) ORDER BY id UNIQUE KEY (id,id2);

set enable_optimizer=1;
set enforce_round_robin=1;
-- set enable_parallel_input_generator=1;

explain select distinct id
        from
            (
                select distinct id from test_distinct.unique_1
                union all
                select distinct id from test_distinct.unique_2
            )id settings enable_distinct_remove=1;

explain select distinct id
        from
            (
                select distinct id from test_distinct.unique_1
                union all
                select distinct id from test_distinct.unique_2
            )id settings enable_distinct_remove=0;

DROP TABLE IF EXISTS test_distinct.unique_1;
DROP TABLE IF EXISTS test_distinct.unique_2;
DROP database if exists test_distinct;
