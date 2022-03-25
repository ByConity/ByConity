drop table if exists test.test_ingest_source;
drop table if exists test.test_ingest_target;

CREATE TABLE test.test_ingest_target (`p_date` Date, `uid` Int32, `event` String, `name` String) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192, enable_ingest_wide_part = 1;
CREATE TABLE test.test_ingest_source (`p_date` Date, `uid` Int32, `event` String, `name` String) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192, enable_ingest_wide_part = 1;

insert into test.test_ingest_target select '2021-01-01', number, 'a', 'b' from numbers(10);
insert into test.test_ingest_source select '2021-01-01', number + 1, 'a', 'b' from numbers(10);

alter table test.test_ingest_target ingest partition '2021-01-01' columns event key uid from test.test_ingest_source settings parallel_ingest_threads = 1;
alter table test.test_ingest_target ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_source settings parallel_ingest_threads = 8;

drop table if exists test.test_ingest_source;
drop table if exists test.test_ingest_target;

CREATE TABLE test.test_ingest_target (`p_date` Date, `uid` Int32, `event` String, `name` String) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;
CREATE TABLE test.test_ingest_source (`p_date` Date, `uid` Int32, `event` String, `name` String) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;

insert into test.test_ingest_target values ('2021-01-01', 1, 'a', 'b'), ('2021-01-01', 2, 'a', 'b');
insert into test.test_ingest_source values ('2021-01-01', 1, 'c', 'b'), ('2021-01-01', 2, 'c', 'b');

alter table test.test_ingest_target ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_source;

select * from test.test_ingest_target order by uid;

drop table if exists test.test_ingest_source;
drop table if exists test.test_ingest_target;
