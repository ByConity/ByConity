set dialect_type='ANSI';
set enable_optimizer=1;

CREATE DATABASE IF NOT EXISTS test;
USE test;

drop table if exists tt1_;
drop table if exists tt2_;

create table tt1_(a Int8) engine = CnchMergeTree() order by a;
create table tt2_(b Int8) engine = CnchMergeTree() order by b;


insert into tt1_ values (1) (3) (5);
insert into tt2_ values (1) (2) (4) (6);

-- { echoOn }

select
    a, (select count(*) from tt2_ where a = b)
from tt1_
order by a;

select
    a, (select count(*) from tt2_ where a + 1 = b)
from tt1_
order by a;

select
    a, (select count(*) from tt2_ where a = b + 1)
from tt1_
order by a;

select
    a, (select count(*) from tt2_ where a + 1 = b + 1)
from tt1_
order by a;

select
    a, (select count(*) from tt2_ where toInt64(a + 1) = b + 1)
from tt1_
order by a;

select
    a, (select count(*) from tt2_ where a + 1 = toInt64(b + 1))
from tt1_
order by a;

select
    a, a in (select b - 1 from tt2_ where a + 1 = b)
from tt1_
order by a;

select
    a, exists(select * from tt2_ where a = b + 1)
from tt1_
order by a;

select
    a, a > any (select b from tt2_ where a + 1 = b)
from tt1_
order by a;

-- { echoOff }

drop table if exists tt1_;
drop table if exists tt2_;
