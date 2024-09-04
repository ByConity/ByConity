set allow_experimental_object_type = 1;
set enable_optimizer=1;
set optimize_json_function_to_subcolumn=1;
set block_json_query_in_optimizer=0;

drop table if exists t40102_json;

create table t40102_json(json JSON) Engine=CnchMergeTree() order by tuple();
insert into t40102_json values ('{"a": 1, "b": 2}');

-- explain stats = 0 select 1 from t40102_json where get_json_object(json, '$.z') = '1'
-- settings allow_nonexist_object_subcolumns=1, enable_simplify_expression_by_derived_constant = 1;

drop table if exists t40102_json;
