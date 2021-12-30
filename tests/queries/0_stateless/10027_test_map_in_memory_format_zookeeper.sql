set database_atomic_wait_for_drop_and_detach_synchronously = 1;
drop table if exists test.10027_test_map_in_memory_format_zookeeper1;
drop table if exists test.10027_test_map_in_memory_format_zookeeper2;

create table test.10027_test_map_in_memory_format_zookeeper1(
       id Int64,
       `string_params` Map(String, String),
       `string_list_params` Map(String, Array(String)),
       `int_params` Map(String, Int32),
       `int_list_params` Map(String, Array(Int32))
) ENGINE = HaMergeTree('/clickhouse/test/tables/10027_test_map_in_memory_format_zookeeper1', '1') ORDER BY id SETTINGS index_granularity = 8192, enable_compact_map_data = 1, min_bytes_for_compact_part = 10000;

create table test.10027_test_map_in_memory_format_zookeeper2(
       id Int64,
       `string_params` Map(String, String),
       `string_list_params` Map(String, Array(String)),
       `int_params` Map(String, Int32),
       `int_list_params` Map(String, Array(Int32))
) ENGINE = HaMergeTree('/clickhouse/test/tables/10027_test_map_in_memory_format_zookeeper1', '2') ORDER BY id SETTINGS index_granularity = 8192, enable_compact_map_data = 0, min_bytes_for_compact_part = 10000, ha_queue_update_sleep_ms=1000;;

set use_uncompressed_cache = 0;

insert into test.10027_test_map_in_memory_format_zookeeper1 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {'i1': 1, 'i2': 2}, {'i_l1':[1, 2, 3], 'i_l2':[4,5,6]});

select 'set use_uncompressed_cache = 0 and write data in memory format';

select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_memory_format_zookeeper1';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_memory_format_zookeeper1 order by id;
select '';
select 'select id, int_params, int_params{\'i1\'}, int_list_params from test.10027_test_map_in_memory_format_zookeeper1';
select id, int_params, int_params{'i1'}, int_list_params from test.10027_test_map_in_memory_format_zookeeper1 order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_memory_format_zookeeper1\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_memory_format_zookeeper1' and active = 1;

SYSTEM SYNC REPLICA test.10027_test_map_in_memory_format_zookeeper2;
select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_memory_format_zookeeper2';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_memory_format_zookeeper2 order by id;
select '';
select 'select id, int_params, int_params{\'i1\'}, int_list_params from test.10027_test_map_in_memory_format_zookeeper2';
select id, int_params, int_params{'i1'}, int_list_params from test.10027_test_map_in_memory_format_zookeeper2 order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_memory_format_zookeeper2\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_memory_format_zookeeper2' and active = 1;


set use_uncompressed_cache = 1;
insert into test.10027_test_map_in_memory_format_zookeeper2 values(1001, {'s3':'s_v3', 's4':'s_v4'}, {'s_l2':['l_v4', 'l_v5']}, {'i2': 1, 'i3': 3}, {'i_l3':[4, 5, 6, 7]});

SYSTEM SYNC REPLICA test.10027_test_map_in_memory_format_zookeeper1;
select '--------------------------------------------------------------';
select 'set use_uncompressed_cache = 1 and write data in memory format';

select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_memory_format_zookeeper1';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_memory_format_zookeeper1 order by id;
select '';
select 'select id, int_params, int_params{\'i1\'}, int_list_params from test.10027_test_map_in_memory_format_zookeeper1';
select id, int_params, int_params{'i1'}, int_list_params from test.10027_test_map_in_memory_format_zookeeper1 order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_memory_format_zookeeper1\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_memory_format_zookeeper1' and active = 1;

select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_memory_format_zookeeper2';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_memory_format_zookeeper2 order by id;
select '';
select 'select id, int_params, int_params{\'i1\'}, int_list_params from test.10027_test_map_in_memory_format_zookeeper2';
select id, int_params, int_params{'i1'}, int_list_params from test.10027_test_map_in_memory_format_zookeeper2 order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_memory_format_zookeeper2\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_memory_format_zookeeper2' and active = 1;

drop table test.10027_test_map_in_memory_format_zookeeper1;
drop table test.10027_test_map_in_memory_format_zookeeper2;
