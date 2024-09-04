set allow_experimental_object_type = 1;
set block_json_query_in_optimizer=0;

create database if not exists test;

drop table if exists test.t40101_json_multi_instances;

create table test.t40101_json_multi_instances (k Int32, json JSON) Engine=CnchMergeTree() partition by k order by tuple() format Null;

-- make sure each node has different json schema
insert into test.t40101_json_multi_instances select number as k, ('{"' || char(97 + (number % 10)) || '": 1}') as json from system.numbers limit 10;

select k, json from test.t40101_json_multi_instances order by k;
-- select json.c from t40101_json settings allow_nonexist_object_subcolumns=1;

drop table if exists test.t40101_json_multi_instances;

