set enable_optimizer = 1;

create database if not exists db48036;
create database if not exists db48036_2;

drop table if exists db48036.test;

drop table if exists db48036_2.test2;

create table db48036.test(a Int32) engine = CnchMergeTree() order by tuple();

create table db48036_2.test2(a Int32) engine = CnchMergeTree() order by tuple();

use db48036;

drop prepared statement if exists ps48036_1;
drop prepared statement if exists ps48036_2;
drop prepared statement if exists ps48036_3;
drop prepared statement if exists ps48036_4;

create prepared statement ps48036_1 as
select * from test;
show create prepared statement ps48036_1;
select '';

create prepared statement ps48036_2 as
select t1.a
from test t1
join (
    select t2.a as a
    from db48036_2.test2 t2
    join (
        select * from db48036.test
        union all
        select * from test
    ) t3 on t2.a = t3.a
) t on t1.a = t.a;
show create prepared statement ps48036_2;
select '';

create prepared statement ps48036_3 as
select (select max(a) from test), 1 in test;
show create prepared statement ps48036_3;
select '';

create prepared statement ps48036_4 as
with t1 as (select 1)
select 1
from test, t1, (
    select 1
    from test, t1, (
        with test as (select 2)
        select 1
        from test, t1
    )
);
show create prepared statement ps48036_4;
select '';

drop prepared statement if exists ps48036_1;
drop prepared statement if exists ps48036_2;
drop prepared statement if exists ps48036_3;
drop prepared statement if exists ps48036_4;

drop table if exists db48036.test;

drop table if exists db48036_2.test2;
