drop table if exists `10027_test_query_map`;

-- limit to read 1 row per block
set preferred_block_size_bytes = 1;

select 'insert a data part whose mark number is bigger than 1, all map columns are empty';
create table `10027_test_query_map`(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(String, Int32),
    `int_list_params` Map(String, Array(Nullable(Int32)))
) ENGINE = CnchMergeTree ORDER BY id SETTINGS index_granularity = 2, enable_compact_map_data = 0, min_bytes_for_wide_part = 0;

-- last mark is incomplete
insert into `10027_test_query_map` (id) select number from system.numbers limit 5;

select count(id) from `10027_test_query_map`;
select count(string_params) from `10027_test_query_map`;
select count(string_list_params) from `10027_test_query_map`;
select count(int_params) from `10027_test_query_map`;
select count(int_list_params) from `10027_test_query_map`;
select count(string_params) from `10027_test_query_map` prewhere id = 0;
select count(string_params) from `10027_test_query_map` prewhere id = 1;
select count(string_params) from `10027_test_query_map` prewhere id < 5;

drop table `10027_test_query_map`;

select '';
select 'insert a data part whose mark number is bigger than 1';
create table `10027_test_query_map`(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(String, Int32),
    `int_list_params` Map(String, Array(Nullable(Int32)))
) ENGINE = CnchMergeTree ORDER BY id SETTINGS index_granularity = 2, enable_compact_map_data = 0, min_bytes_for_wide_part = 0;

-- last mark is incomplete
insert into `10027_test_query_map` values (1, {'1': '1'}, {'1': ['1']}, {'1': 1}, {'1': [1, null]}) (2, {'2': '1'}, {'2': ['1']}, {'2': 1}, {'2': [1, null]}) (3, {'3': '1'}, {'3': ['1']}, {'3': 1}, {'3': [1, null]});

-- test reading map sequentially
select * from `10027_test_query_map` order by id;

drop table `10027_test_query_map`;
