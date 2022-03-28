set database_atomic_wait_for_drop_and_detach_synchronously = 1;

drop table if exists test.test_ingest_map_target_lc_1;
drop table if exists test.test_ingest_map_target_lc_2;
drop table if exists test.test_ingest_map_source_lc;

CREATE TABLE test.test_ingest_map_target_lc_1 (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, LowCardinality(Nullable(String)))) ENGINE = HaMergeTree('/clickhouse/test/test/ingest_map_target_lc/1', '1') PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192, enable_ingest_wide_part = 1;
CREATE TABLE test.test_ingest_map_target_lc_2 (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, LowCardinality(Nullable(String)))) ENGINE = HaMergeTree('/clickhouse/test/test/ingest_map_target_lc/1', '2') PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192, enable_ingest_wide_part = 1;

insert into test.test_ingest_map_target_lc_1 values ('2022-01-01', 1, 'a', {}), ('2022-01-01', 2, 'a', {}), ('2022-01-01', 3, 'a', {}), ('2022-01-01', 4, 'a', {});

CREATE TABLE test.test_ingest_map_source_lc (`p_date` Date, `uid` Int32, `c1` String, `string_profile` Map(String, LowCardinality(Nullable(String)))) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192, enable_ingest_wide_part = 1;

insert into test.test_ingest_map_source_lc values ('2022-01-01', 1, 'a', {'1':'x'}), ('2022-01-01', 3, 'a', {'2':'y'}), ('2022-01-01', 5, 'a', {'2': ''});

alter table test.test_ingest_map_target_lc_1 ingest partition '2022-01-01' columns string_profile{'1'}, string_profile{'2'}, string_profile{'3'} key uid from test.test_ingest_map_source_lc;

drop table if exists test.test_ingest_map_target_lc_1;
drop table if exists test.test_ingest_map_target_lc_2;
drop table if exists test.test_ingest_map_source_lc;