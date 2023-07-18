drop table if exists t1;
drop table if exists t1_local;
drop table if exists t2;
drop table if exists t2_local;

create table t1_local(a Int8) engine = MergeTree() order by tuple();
create table t1 as t1_local engine = Distributed(test_shard_localhost, currentDatabase(), 't1_local');
create table t2_local(b Int8) engine = MergeTree() order by tuple();
create table t2 as t2_local engine = Distributed(test_shard_localhost, currentDatabase(), 't2_local');

insert into t1_local values (1);
insert into t2_local values (1);

-- { echoOn }

select count(*) from t1, t2 where a = b;
select count(*) from t1, t2 where toInt32(a) = toInt64(b);
select count(*) from t1, t2 where toInt32(a + 1) = toInt64(b + 1);

-- { echoOff }

drop table if exists t1;
drop table if exists t1_local;
drop table if exists t2;
drop table if exists t2_local;
