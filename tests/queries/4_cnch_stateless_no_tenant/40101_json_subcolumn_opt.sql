set allow_experimental_object_type = 1;
set enable_optimizer=1;
set optimize_json_function_to_subcolumn=1;
set block_json_query_in_optimizer=0;

drop table if exists t40101_json;

create table t40101_json(json JSON) Engine=CnchMergeTree() order by tuple();
insert into t40101_json values ('{"a": 1, "b": 2}');

explain stats=0 select get_json_object(json, '$.a') as a, get_json_object(json, '$.z') as z from t40101_json;

select get_json_object(json, '$.a') as a, get_json_object(json, '$.z') as z from t40101_json;

explain stats=0 select JSONExtractRaw(json, 'a') as a, JSONExtractRaw(json, 'z') as z from t40101_json;

select JSONExtractRaw(json, 'a') as a, JSONExtractRaw(json, 'z') as z from t40101_json;

drop table if exists t40101_json;
