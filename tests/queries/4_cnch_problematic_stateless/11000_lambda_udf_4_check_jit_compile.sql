create function test.TestMyFunc as (a, b, c) -> a + b > c AND c < 10;
select test.TestMyFunc(1, 2, 3);
select test.TestMyFunc(2, 2, 3);
select test.TestMyFunc(20, 20, 11);
drop function test.TestMyFunc;