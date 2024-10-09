set dialect_type='ANSI';
set enable_optimizer=1;

drop table if exists tt1;
drop table if exists tt2;

create table tt1(a Int8) engine = CnchMergeTree() order by a;
create table tt2(b Int8) engine = CnchMergeTree() order by b;

insert into tt1 values (1) (3) (5);
insert into tt2 values (1) (2) (4) (6);

-- { echoOn }

select
    a, (select count(*) from tt2 where a = b)
from tt1
order by a;

select
    a, (select count(*) from tt2 where a + 1 = b)
from tt1
order by a;

select
    a, (select count(*) from tt2 where a = b + 1)
from tt1
order by a;

select
    a, (select count(*) from tt2 where a + 1 = b + 1)
from tt1
order by a;

select
    a, (select count(*) from tt2 where toInt64(a + 1) = b + 1)
from tt1
order by a;

select
    a, (select count(*) from tt2 where a + 1 = toInt64(b + 1))
from tt1
order by a;

select
    a, a in (select b - 1 from tt2 where a + 1 = b)
from tt1
order by a;

select
    a, exists(select * from tt2 where a = b + 1)
from tt1
order by a;

select
    a, a > any (select b from tt2 where a + 1 = b)
from tt1
order by a;

-- { echoOff }

drop table if exists tt1;
drop table if exists tt2;
