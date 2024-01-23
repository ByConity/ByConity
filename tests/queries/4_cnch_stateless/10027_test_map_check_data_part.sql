set disable_optimize_final = 0;
set check_query_single_value_result = 1;

drop table if exists 10027_test_map_check_data_part1;
drop table if exists 10027_test_map_check_data_part2;
drop table if exists 10027_test_map_check_data_part3;
drop table if exists 10027_test_map_check_data_part4;

-- create table contains map in wide part format use horizontal merge
create table 10027_test_map_check_data_part1(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(Int32, Int32),
    `int_list_params` Map(Int32, Array(Int32))
) 
engine = CnchMergeTree
order by id 
settings enable_vertical_merge_algorithm = 0;

system start merges 10027_test_map_check_data_part1;

-- create table contains map in wide part format use vertical merge
create table 10027_test_map_check_data_part2(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(Int32, Int32),
    `int_list_params` Map(Int32, Array(Int32))
) 
engine = CnchMergeTree
order by id 
settings enable_vertical_merge_algorithm = 1;

system start merges 10027_test_map_check_data_part2;

-- create table contains kv map
create table 10027_test_map_check_data_part3(
    id Int64,
    `string_params` Map(String, String) KV,
    `string_list_params` Map(String, Array(String)) KV,
    `int_params` Map(Int32, Int32) KV,
    `int_list_params` Map(Int32, Array(Int32)) KV
) 
engine = CnchMergeTree
order by id;

system start merges 10027_test_map_check_data_part3;

-- create table contains low cardinality map
create table 10027_test_map_check_data_part4(
    id Int64,
    `string_params` Map(String, LowCardinality(Nullable(String))),
    `int_params` Map(Int32, LowCardinality(Nullable(Int32)))
) 
engine = CnchMergeTree
order by id;

system start merges 10027_test_map_check_data_part4;


insert into 10027_test_map_check_data_part1 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into 10027_test_map_check_data_part1 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into 10027_test_map_check_data_part2 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into 10027_test_map_check_data_part2 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into 10027_test_map_check_data_part3 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into 10027_test_map_check_data_part3 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into 10027_test_map_check_data_part4 select number, map('k1', toString(number)), map(1, number) from system.numbers limit 10;
insert into 10027_test_map_check_data_part4 select number, map('k1', toString(number)), map(1, number) from system.numbers limit 100001;

check table 10027_test_map_check_data_part1;
check table 10027_test_map_check_data_part2;
check table 10027_test_map_check_data_part3;
check table 10027_test_map_check_data_part4;

optimize table 10027_test_map_check_data_part1 final settings mutations_sync=1;
optimize table 10027_test_map_check_data_part2 final settings mutations_sync=1;
optimize table 10027_test_map_check_data_part3 final settings mutations_sync=1;
optimize table 10027_test_map_check_data_part4 final settings mutations_sync=1;

-- optimization maybe unfinished even if add mutations_sync=1, sleep here is slow but can make ci stable
select sleepEachRow(3) from numbers(25) format Null;

check table 10027_test_map_check_data_part1;
check table 10027_test_map_check_data_part2;
check table 10027_test_map_check_data_part3;
check table 10027_test_map_check_data_part4;

select * from 10027_test_map_check_data_part1;
select * from 10027_test_map_check_data_part2;
select * from 10027_test_map_check_data_part3;
-- select * from 10027_test_map_check_data_part4;

drop table 10027_test_map_check_data_part1;
drop table 10027_test_map_check_data_part2;
drop table 10027_test_map_check_data_part3;
drop table 10027_test_map_check_data_part4;
