set enable_optimizer=1;

create database if not exists test;

drop table if exists t1;
drop table if exists t2;

create table t1(a Int8) engine = CnchMergeTree() order by tuple();
create table t2(b Int8) engine = CnchMergeTree() order by tuple();

insert into t1 values (1);
insert into t2 values (1);

-- { echoOn }

select count(*) from t1, t2 where a = b;
select count(*) from t1, t2 where toInt32(a) = toInt64(b);
select count(*) from t1, t2 where toInt32(a + 1) = toInt64(b + 1);

-- { echoOff }

drop table if exists t1;
drop table if exists t2;
