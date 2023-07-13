use test;

drop table if exists t48020_local;
drop table if exists t48020;

create table t48020_local (
  a UInt64,
  b String
) engine = MergeTree() order by a;

create table t48020 as t48020_local engine = Distributed(test_shard_localhost, currentDatabase(), 't48020_local');

select count() from (select a from t48020 group by a limit 0);

select count() from (select a from t48020 limit 0) group by a;

select count() from (select a from t48020 group by a limit 0) group by a;

set enable_optimizer = 1;

explain select count() from (select a from t48020 group by a limit 0);

explain select count() from (select a from t48020 limit 0) group by a;

explain select count() from (select a from t48020 group by a limit 0) group by a;

drop table if exists t48020_local;
drop table if exists t48020;
