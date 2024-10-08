drop table if exists jit_tests;

set compile_expressions=1;
set min_count_to_compile_expression=0;

CREATE TABLE jit_tests(`string_id` String, `string_nullable_id` Nullable(String)) ENGINE = CnchMergeTree PARTITION BY tuple() ORDER BY tuple() SETTINGS index_granularity = 8192;

INSERT INTO jit_tests values('1', '1'),('11', '11'),('111', '111'),('1111', '1111'),('11111', '11111'),('111111', '111111'),('1111111', '1111111'),('11111111', '11111111'),('111111111', NULL);

select multiIf(length(string_id) > 0 and length(string_id) <= 3, 1, length(string_id) > 3 and length(string_id) <= 6, 2, length(string_id) > 6 and length(string_id) <= 9, 3, 0) from jit_tests;

select multiIf(string_id = '1', 'Others', string_id = '11', 'xxx', string_nullable_id) from jit_tests;

drop table if exists jit_tests;