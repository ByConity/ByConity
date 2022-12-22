drop table if exists test.test_ingest_empty_source;
drop table if exists test.test_ingest_empty_target;

CREATE TABLE test.test_ingest_empty_target (`p_date` Date, `uid` Int32, `event` String, `name` String) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;
CREATE TABLE test.test_ingest_empty_source (`p_date` Date, `uid` Int32, `event` String, `name` String) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;

insert into test.test_ingest_empty_source select '2021-01-01', number, 'a', 'b' from numbers(100000);

alter table test.test_ingest_empty_target ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_empty_source;

select count() from test.test_ingest_empty_target;

alter table test.test_ingest_empty_target ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_empty_source settings allow_ingest_empty_partition = 1;

select count() from test.test_ingest_empty_target;

drop table if exists test.test_ingest_empty_source;
drop table if exists test.test_ingest_empty_target;