set dialect_type='MYSQL';
drop table if exists test_dual;
create table test_dual(id tinyint(32));
insert into test_dual values (1), (2), (3);
select min(7) from dual;
select sqrt(25) from dual;
-- adb also doesn't support
-- select min(7) from dual, test_dual;
-- select min(7) from test_dual, dual;
drop table test_dual;
