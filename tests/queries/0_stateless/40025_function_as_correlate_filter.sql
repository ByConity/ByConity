set enable_optimizer=1;

drop table if exists t1;
drop table if exists t1_local;
drop table if exists t2;
drop table if exists t2_local;

create table t1_local(a Int8) engine = MergeTree() order by tuple();
create table t1 as t1_local engine = Distributed(test_shard_localhost, currentDatabase(), 't1_local');
create table t2_local(b Int8) engine = MergeTree() order by tuple();
create table t2 as t2_local engine = Distributed(test_shard_localhost, currentDatabase(), 't2_local');

insert into t1_local values (1) (3) (5);
insert into t2_local values (1) (2) (4) (6);

-- { echoOn }

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
    a, exists(select * from t2 where a = b + 1)
from t1
order by a;

select
    a, a > any (select b from t2 where a + 1 = b)
from t1
order by a;

-- { echoOff }

drop table if exists t1;
drop table if exists t1_local;
drop table if exists t2;
drop table if exists t2_local;