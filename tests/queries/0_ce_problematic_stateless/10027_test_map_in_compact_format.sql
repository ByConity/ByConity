drop table if exists test.10027_test_map_in_compact_format;

create table test.10027_test_map_in_compact_format(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(Int32, Int32),
    `int_list_params` Map(Int32, Array(Int32))
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, min_bytes_for_compact_part = 0, enable_compact_map_data = 1;

set use_uncompressed_cache = 0;
set optimize_map_column_serialization = 0;

insert into test.10027_test_map_in_compact_format values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});

select 'set use_uncompressed_cache = 0, optimize_map_column_serialization = 0 and write data in compact format';

select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_compact_format';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_compact_format order by id;
select '';
select 'select id, int_params, int_params{1}, int_list_params from test.10027_test_map_in_compact_format';
select id, int_params, int_params{1}, int_list_params from test.10027_test_map_in_compact_format order by id;
select '';
select 'select name, part_type from system.parts where table = \'10027_test_map_in_compact_format\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_compact_format' and active = 1;

set use_uncompressed_cache = 1;
set optimize_map_column_serialization = 1;
insert into test.10027_test_map_in_compact_format values(1001, {'s3':'s_v3', 's4':'s_v4'}, {'s_l2':['l_v4', 'l_v5']}, {2: 1, 3: 3}, {3:[4, 5, 6, 7]});

select '--------------------------------------------------------------';
select 'set use_uncompressed_cache = 1, optimize_map_column_serialization = 1 and write data in compact format';

select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_compact_format';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_compact_format order by id;
select '';
select 'select id, int_params, int_params{1}, int_list_params from test.10027_test_map_in_compact_format';
select id, int_params, int_params{1}, int_list_params from test.10027_test_map_in_compact_format order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_compact_format\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_compact_format' and active = 1;

select '--------------------------------------------------------------';
select 'trigger merge action, it will generate parts in compact format';

optimize table test.10027_test_map_in_compact_format final;
select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_compact_format';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_compact_format order by id;
select '';
select 'select id, int_params, int_params{1}, int_list_params from test.10027_test_map_in_compact_format';
select id, int_params, int_params{1}, int_list_params from test.10027_test_map_in_compact_format order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_compact_format\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_compact_format' and active = 1;

select '-------------------------------------------------------------';
select 'alter settings and trigger merge action, it will generate parts in wide format';

alter table test.10027_test_map_in_compact_format modify setting min_bytes_for_compact_part = 0, min_bytes_for_wide_part = 0;
optimize table test.10027_test_map_in_compact_format final;
select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_compact_format';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_compact_format order by id;
select '';
select 'select id, int_params, int_params{1}, int_list_params from test.10027_test_map_in_compact_format';
select id, int_params, int_params{1}, int_list_params from test.10027_test_map_in_compact_format order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_compact_format\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_compact_format' and active = 1;

select '-------------------------------------------------------------';
select 'alter settings and trigger merge action, it will generate parts in uncompacted map format';

alter table test.10027_test_map_in_compact_format modify setting enable_compact_map_data = 0;
insert into test.10027_test_map_in_compact_format values(1002, {'s4':'s_v3', 's5':'s_v5'}, {'s_l2':['l_2'], 's_l3': ['l_3']}, {4: 4}, {4:[6, 7]});
optimize table test.10027_test_map_in_compact_format final;
select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_compact_format';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_compact_format order by id;
select '';
select 'select id, int_params, int_params{1}, int_list_params from test.10027_test_map_in_compact_format';
select id, int_params, int_params{1}, int_list_params from test.10027_test_map_in_compact_format order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_compact_format\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_compact_format' and active = 1;

drop table test.10027_test_map_in_compact_format;
