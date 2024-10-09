drop table if exists t48020;

create table t48020 (
  a UInt64,
  b String
) engine = CnchMergeTree() order by a;

select count() from (select a from t48020 group by a limit 0);

select count() from (select a from t48020 limit 0) group by a;

select count() from (select a from t48020 group by a limit 0) group by a;

set enable_optimizer = 1;

explain select count() from (select a from t48020 group by a limit 0);

explain select count() from (select a from t48020 limit 0) group by a;

explain select count() from (select a from t48020 group by a limit 0) group by a;

drop table if exists t48020;
