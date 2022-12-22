set database_atomic_wait_for_drop_and_detach_synchronously = 1;

drop table if exists test.test_ingest_map_target;
drop table if exists test.test_ingest_map_source;

CREATE TABLE test.test_ingest_map_target (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, String)) ENGINE = HaMergeTree('/clickhouse/test/test/ingest_map/1', '1') PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;

CREATE TABLE test.test_ingest_map_source (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, String)) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;

insert into test.test_ingest_map_target select '2021-01-01', number, 'a', map('k', 'v') from numbers(5);
insert into test.test_ingest_map_source select '2021-01-01', number, 'b', map('k', 'w') from numbers(5);

alter table test.test_ingest_map_target ingest partition '2021-01-01' columns c1, string_profile{'k'} key uid from test.test_ingest_map_source;

select * from test.test_ingest_map_target order by uid;

drop table if exists test.test_ingest_map_target;
drop table if exists test.test_ingest_map_source;

CREATE TABLE test.test_ingest_map_target (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, String)) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;

CREATE TABLE test.test_ingest_map_source (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, String)) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;

insert into test.test_ingest_map_target select '2021-01-01', number, 'a', map('k', 'v') from numbers(5);
insert into test.test_ingest_map_source select '2021-01-01', number, 'b', map('c', 'w') from numbers(5);

alter table test.test_ingest_map_target ingest partition '2021-01-01' columns c1, string_profile{'c'} key uid from test.test_ingest_map_source;

select * from test.test_ingest_map_target order by uid;

alter table test.test_ingest_map_target drop partition id '20210101';

insert into test.test_ingest_map_target values ('2021-01-01', 1, 'a', {});

alter table test.test_ingest_map_target ingest partition '2021-01-01' columns c1, string_profile{'c'} key uid from test.test_ingest_map_source;

select * from test.test_ingest_map_target order by uid;

drop table if exists test.test_ingest_map_target;
drop table if exists test.test_ingest_map_source;

drop table if exists test.test_ingest_map_target_1;
drop table if exists test.test_ingest_map_target_2;
drop table if exists test.test_ingest_map_source;

CREATE TABLE test.test_ingest_map_target_1 (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, String)) ENGINE = HaMergeTree('/clickhouse/test/test/ingest_map_target/1', '1') PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;
CREATE TABLE test.test_ingest_map_target_2 (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, String)) ENGINE = HaMergeTree('/clickhouse/test/test/ingest_map_target/1', '2') PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;

CREATE TABLE test.test_ingest_map_source (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, String)) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;

insert into test.test_ingest_map_target_1 values ('2022-01-01', 1, 'a', {}), ('2022-01-01', 2, 'a', {}), ('2022-01-01', 3, 'a', {}), ('2022-01-01', 4, 'a', {});
insert into test.test_ingest_map_source values ('2022-01-01', 1, 'a', {'1':'x'}), ('2022-01-01', 3, 'a', {'2':'y'}), ('2022-01-01', 5, 'a', {'2': ''});

alter table test.test_ingest_map_target_1 ingest partition '2022-01-01' columns string_profile{'1'}, string_profile{'2'}, string_profile{'3'} key uid from test.test_ingest_map_source;

drop table if exists test.test_ingest_map_target_1;
drop table if exists test.test_ingest_map_target_2;
drop table if exists test.test_ingest_map_source;