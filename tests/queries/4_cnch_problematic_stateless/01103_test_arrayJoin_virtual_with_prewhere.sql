drop table if exists test_map_local;

CREATE TABLE test_map_local (`p_date` Date, `id` Int32, `string_map` Map(String, String), `event` String, `int_map` Map(Int32, Int32)) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192;

insert into test_map_local values ('2021-01-01', 1, {'a':'b'}, 'a', {1:1});

select arrayJoin(_map_column_keys) from test_map_local where p_date = '2021-01-01';
select arrayJoin(_map_column_keys) from test_map_local where p_date = '2021-01-01' and event = 'a';
select arrayJoin([_partition_id]) from test_map_local where p_date = '2021-01-01';
select arrayJoin([_partition_id]) from test_map_local where p_date = '2021-01-01' and event = 'a';

drop table if exists test_map_local;
