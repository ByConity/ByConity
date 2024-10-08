drop table if exists jit_tests6;

CREATE TABLE jit_tests6(`int_id` Int64, `nullable_int_id` Nullable(Int64)) ENGINE = CnchMergeTree PARTITION BY tuple() ORDER BY tuple() SETTINGS index_granularity = 8192;

INSERT INTO jit_tests6 values(1, NULL), (1, 2);

select 'true and null';
select int_id, nullable_int_id, nullable_int_id and int_id = 1 from jit_tests6 settings max_threads=1, compile_expressions=0, min_count_to_compile_expression=0;
select int_id, nullable_int_id, nullable_int_id and int_id = 1 from jit_tests6 settings max_threads=1, compile_expressions=1, min_count_to_compile_expression=0;
select int_id, nullable_int_id, int_id = 1 and nullable_int_id from jit_tests6 settings max_threads=1, compile_expressions=1, min_count_to_compile_expression=0;


select 'true or null';
select int_id, nullable_int_id, nullable_int_id or int_id = 1 from jit_tests6 settings max_threads=1, compile_expressions=0, min_count_to_compile_expression=0;
select int_id, nullable_int_id, nullable_int_id or int_id = 1 from jit_tests6 settings max_threads=1, compile_expressions=1, min_count_to_compile_expression=0;
select int_id, nullable_int_id, int_id = 1 or nullable_int_id from jit_tests6 settings max_threads=1, compile_expressions=1, min_count_to_compile_expression=0;


select 'false and null';
select int_id, nullable_int_id, nullable_int_id and int_id != 1 from jit_tests6 settings max_threads=1, compile_expressions=0, min_count_to_compile_expression=0;
select int_id, nullable_int_id, nullable_int_id and int_id != 1 from jit_tests6 settings max_threads=1, compile_expressions=1, min_count_to_compile_expression=0;
select int_id, nullable_int_id, int_id != 1 and nullable_int_id from jit_tests6 settings max_threads=1, compile_expressions=1, min_count_to_compile_expression=0;

select 'false or null';
select int_id, nullable_int_id, nullable_int_id or int_id != 1 from jit_tests6 settings max_threads=1, compile_expressions=0, min_count_to_compile_expression=0;
select int_id, nullable_int_id, nullable_int_id or int_id != 1 from jit_tests6 settings max_threads=1, compile_expressions=1, min_count_to_compile_expression=0;
select int_id, nullable_int_id, int_id != 1 or nullable_int_id from jit_tests6 settings max_threads=1, compile_expressions=1, min_count_to_compile_expression=0;

