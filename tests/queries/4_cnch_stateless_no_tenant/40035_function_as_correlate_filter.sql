set dialect_type='ANSI';
set enable_optimizer=1;
set enable_nested_loop_join=1;

create database if not exists test;

drop table if exists t1;
drop table if exists t2;

create table t1(a Int8) engine = CnchMergeTree() order by tuple();
create table t2(b Int8) engine = CnchMergeTree() order by tuple();

insert into t1 values (1) (3) (5);
insert into t2 values (1) (2) (4) (6);


select
    a, (select count(*) from t2 where a = b)
from t1
order by a;

select
    a, (select count(*) from t2 where a + 1 = b)
from t1
order by a;

select
    a, (select count(*) from t2 where a = b + 1)
from t1
order by a;

select
    a, (select count(*) from t2 where a + 1 = b + 1)
from t1
order by a;

select
    a, (select count(*) from t2 where toInt64(a + 1) = b + 1)
from t1
order by a;

select
    a, (select count(*) from t2 where a + 1 = toInt64(b + 1))
from t1
order by a;

select
    a, a in (select b - 1 from t2 where a + 1 = b)
from t1
order by a;

select
    a, exists((select * from t2 where a = b + 1))
from t1
order by a;

select
    a, a > any (select b from t2 where a + 1 = b)
from t1
order by a;

-- { echoOff }

drop table if exists t1;
drop table if exists t2;
