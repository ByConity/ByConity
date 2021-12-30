drop table if exists test.10027_test_map_in_wide_format;

create table test.10027_test_map_in_wide_format(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(String, Int32),
    `int_list_params` Map(String, Array(Int32))
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, enable_compact_map_data = 1, min_bytes_for_wide_part = 0;

set use_uncompressed_cache = 0;

insert into test.10027_test_map_in_wide_format values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {'i1': 1, 'i2': 2}, {'i_l1':[1, 2, 3], 'i_l2':[4,5,6]});

select 'set use_uncompressed_cache = 0 and write data in wide format';

select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_wide_format';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_wide_format order by id;
select '';
select 'select id, int_params, int_params{\'i1\'}, int_list_params from test.10027_test_map_in_wide_format';
select id, int_params, int_params{'i1'}, int_list_params from test.10027_test_map_in_wide_format order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_wide_format\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_wide_format' and active = 1;

set use_uncompressed_cache = 1;
insert into test.10027_test_map_in_wide_format values(1001, {'s3':'s_v3', 's4':'s_v4'}, {'s_l2':['l_v4', 'l_v5']}, {'i2': 1, 'i3': 3}, {'i_l3':[4, 5, 6, 7]});

select '--------------------------------------------------------------';
select 'set use_uncompressed_cache = 1 and write data in wide format';

select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_wide_format';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_wide_format order by id;
select '';
select 'select id, int_params, int_params{\'i1\'}, int_list_params from test.10027_test_map_in_wide_format';
select id, int_params, int_params{'i1'}, int_list_params from test.10027_test_map_in_wide_format order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_wide_format\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_wide_format' and active = 1;

select '--------------------------------------------------------------';
select 'trigger merge action, it will generate parts in wide format';

optimize table test.10027_test_map_in_wide_format final;
select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_wide_format';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_wide_format order by id;
select '';
select 'select id, int_params, int_params{\'i1\'}, int_list_params from test.10027_test_map_in_wide_format';
select id, int_params, int_params{'i1'}, int_list_params from test.10027_test_map_in_wide_format order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_wide_format\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_wide_format' and active = 1;

select '-------------------------------------------------------------';
select 'alter settings and trigger merge action, it will generate parts in uncompacted map format';

alter table test.10027_test_map_in_wide_format modify setting enable_compact_map_data = 0;
insert into test.10027_test_map_in_wide_format values(1002, {'s4':'s_v3', 's5':'s_v5'}, {'s_l2':['l_2'], 's_l3': ['l_3']}, {'i4': 4}, {'i_l4':[6, 7]});
optimize table test.10027_test_map_in_wide_format final;
select '';
select 'select id, string_params, string_params{\'s1\'}, string_list_params from test.10027_test_map_in_wide_format';
select id, string_params, string_params{'s1'}, string_list_params from test.10027_test_map_in_wide_format order by id;
select '';
select 'select id, int_params, int_params{\'i1\'}, int_list_params from test.10027_test_map_in_wide_format';
select id, int_params, int_params{'i1'}, int_list_params from test.10027_test_map_in_wide_format order by id;
select '';
select 'select name, part_type, compact_map from system.parts where table = \'10027_test_map_in_wide_format\' and active = 1';
select name, part_type, compact_map from system.parts where table = '10027_test_map_in_wide_format' and active = 1;

drop table test.10027_test_map_in_wide_format;
