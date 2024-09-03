drop table if exists t1;
CREATE TABLE t1(c1 UInt64, c2 Nullable(String), c3 Int32) ENGINE = CnchMergeTree PARTITION BY c1 ORDER BY c1;

insert into t1 values (1, 'a', 1);
insert into t1 values (2, 'b', 1);
insert into t1 values (3, null, 0);

select count(distinct c2) / sum(c3) from t1;
select count(distinct c2) / sum(c3) from t1 where c1 > 10;

select count(distinct c2, c3) / sum(c3) from t1;
select count(distinct c2, c3) / sum(c3) from t1 where c1 > 10;

set enable_expand_distinct_optimization=1;

select count(distinct c2) / sum(c3) from t1;
select count(distinct c2) / sum(c3) from t1 where c1 > 10;

select count(distinct c2, c3) / sum(c3) from t1;
select count(distinct c2, c3) / sum(c3) from t1 where c1 > 10;

set enable_mark_distinct_optimzation=1;
select count(distinct c2) / sum(c3) from t1;
select count(distinct c2) / sum(c3) from t1 where c1 > 10;

select count(distinct c2, c3) / sum(c3) from t1;
select count(distinct c2, c3) / sum(c3) from t1 where c1 > 10;
