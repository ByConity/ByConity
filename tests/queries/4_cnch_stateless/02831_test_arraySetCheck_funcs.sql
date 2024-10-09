drop table if exists test_array_set_check;

CREATE TABLE test_array_set_check  (vid_array Array(Int64), array_str Array(String), nullable_vid_array Array(Nullable(Int64)), nullable_array_str Array(Nullable(String))) ENGINE = CnchMergeTree() PARTITION BY tuple() ORDER BY tuple() SETTINGS index_granularity = 8192;
insert into test_array_set_check select [1], [1], ['1'], ['1'] from system.numbers limit 10000 settings max_execution_time=0;

select count(1) from test_array_set_check where arraySetCheck(vid_array, (1));
select count(1) from test_array_set_check where arraySetCheck(nullable_vid_array, (1));
select count(1) from test_array_set_check where arraySetCheck(array_str, ('1'));
select count(1) from test_array_set_check where arraySetCheck(nullable_array_str, ('1'));
select arraySetGet(vid_array, (1)) from test_array_set_check limit 1;
select arraySetGet(nullable_vid_array, (1)) from test_array_set_check limit 1;
select arraySetGetAny(vid_array, (1,2)) from test_array_set_check limit 1;
select arraySetGetAny(nullable_vid_array, (1,2)) from test_array_set_check limit 1;

drop table if exists test_array_set_check;