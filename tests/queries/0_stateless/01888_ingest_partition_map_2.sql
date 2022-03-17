set database_atomic_wait_for_drop_and_detach_synchronously = 1;

drop table if exists test.test_ingest_map_target2;
drop table if exists test.test_ingest_map_source2;

CREATE TABLE test.test_ingest_map_target2 (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, String), `int_profile` Map(UInt64, UInt64)) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192, enable_ingest_wide_part = 1;

CREATE TABLE test.test_ingest_map_source2 (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, String), `int_profile` Map(UInt64, UInt64)) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192, enable_ingest_wide_part = 1;

insert into test.test_ingest_map_target2 select '2021-01-01', number, 'a', map('k', 'v'), map(number, number) from numbers(5);
insert into test.test_ingest_map_source2 select '2021-01-01', number, 'b', map('k', 'w'), map(number, number) from numbers(5);

alter table test.test_ingest_map_target2 ingest partition '2021-01-01' columns c1, string_profile{'k'} key uid from test.test_ingest_map_source2;

drop table if exists test.test_ingest_map_target2;
drop table if exists test.test_ingest_map_source2;