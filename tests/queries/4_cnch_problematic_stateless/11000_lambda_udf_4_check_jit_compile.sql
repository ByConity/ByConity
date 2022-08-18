create function TestMyFunc as (a, b, c) -> a + b > c AND c < 10;
select TestMyFunc(1, 2, 3);
select TestMyFunc(2, 2, 3);
select TestMyFunc(20, 20, 11);
drop function TestMyFunc;