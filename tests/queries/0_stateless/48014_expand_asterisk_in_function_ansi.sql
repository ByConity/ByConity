set dialect_type = 'ANSI';
set enable_optimizer = 1;


drop table if exists t48019_local;
drop table if exists t48019;

create table t48019_local (
  somedate Date,
  id UInt64,
  data String
) engine = MergeTree() order by id;

create table t48019 as t48019_local engine = Distributed(test_shard_localhost, currentDatabase(), 't48019_local');

insert into t48019 values ('2022-01-01', 1, '1');
insert into t48019 values ('2023-01-01', 2, '2');
insert into t48019 values ('2024-01-01', 3, '3');

select sum(cityHash64(*)) from t48019;