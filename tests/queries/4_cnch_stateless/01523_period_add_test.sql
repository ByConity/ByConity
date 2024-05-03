select period_add(202201,2);
select period_add('202201','2');
select period_add('202201',2);
select period_add(202201,'2');
select period_add(202201,2.4);
select period_add('202201','2.4');
select period_add('202201',2.5);
select period_add(202201,'2.5');
select period_add(202201,-2);
select period_add('202201','-2');
select period_add('202201',-2);
select period_add(202201,'-2');
select period_add(202201,-2.4);
select period_add('202201','-2.4');
select period_add('202201',-2.5);
select period_add(202201,'-2.5');
select period_add(000001,-2);
select period_add(9805, 1);
select period_add(6901, 1);
select period_add(5, 1);
select period_add(-000001,-2); -- { serverError 377 }

select period_add(202200, number) from (select * from numbers(10));
select period_add(202200, -number) from (select * from numbers(10));