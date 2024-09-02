SET enable_optimizer = 0;
with it as ( select * from numbers(1) ) select it.number, i.number from it as i;
explain syntax with it as ( select * from numbers(1) ) select it.number, i.number from it as i;
