drop function if EXISTS test.lambda_udf_4_jit;
create function test.lambda_udf_4_jit as (a, b, c) -> a + b > c AND c < 10;
select test.lambda_udf_4_jit(1, 2, 3);
select test.lambda_udf_4_jit(2, 2, 3);
select test.lambda_udf_4_jit(20, 20, 11);
drop function test.lambda_udf_4_jit;