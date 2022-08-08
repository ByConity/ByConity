drop table if exists test.test_insert;
drop table if exists test.test_insert_source;

CREATE TABLE test.test_insert (`p_date` Date, `id` Int32) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192;
CREATE TABLE test.test_insert_source (`p_date` Date, `id` Int32) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192;

insert into test.test_insert_source select '2022-01-01', number from numbers(10);

set enable_insert_distributed_stages = 1;
INSERT INTO test.test_insert SELECT *
FROM test.test_insert_source
SETTINGS enable_distributed_stages = 1, fallback_to_simple_query = 0;

select count() from test.test_insert;

drop table if exists test.test_insert;
drop table if exists test.test_insert_source;