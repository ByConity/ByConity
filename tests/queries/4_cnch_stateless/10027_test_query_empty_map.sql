drop table if exists test_map;

select 'insert a data part whose mark number is bigger than 1, all map columns are empty';
create table test_map(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(String, Int32),
    `int_list_params` Map(String, Array(Nullable(Int32)))
) ENGINE = CnchMergeTree ORDER BY id SETTINGS index_granularity = 150, enable_compact_map_data = 0, min_bytes_for_wide_part = 0;

insert into test_map (id) select number from system.numbers limit 500;

select '';
select 'select count(id), count(string_params), count(string_list_params), count(int_params), count(int_list_params) from test_map';
select count(id) from test_map;
select count(string_params) from test_map;
select count(string_list_params) from test_map;
select count(int_params) from test_map;
select count(int_list_params) from test_map;

drop table test_map;
