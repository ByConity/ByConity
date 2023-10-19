set enable_optimizer=1;

drop table if exists t1;
drop table if exists t2;

create table t1_local(a LowCardinality(Nullable(Int8))) engine = MergeTree() order by tuple();
create table t2_local(a LowCardinality(Int8)) engine = MergeTree() order by tuple();
create table t1 as t1_local engine = Distributed(test_shard_localhost, currentDatabase(), t1_local, rand());
create table t2 as t2_local engine = Distributed(test_shard_localhost, currentDatabase(), t2_local, rand());

insert into t1 values (1) (3) (5) (NULL);
insert into t2 values (1) (3) (5);

select a from t1 where a is not null order by a;
select a from t1 where a is null order by a;
select count(distinct a) from t1;
select cast(a, 'Nullable(Int32)') from t1 where cast(a, 'Nullable(Int32)') = 1;
select a from t2 where a is not null order by a;
select t1.a from t1, t2 where t1.a = t2.a order by t1.a;
