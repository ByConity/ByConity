drop table if exists t40037;

create table t40037(
    a UInt64, b UInt64, c UInt64,
    PROJECTION p_norm (select b, c order by b),
    PROJECTION p_agg (select a, b, count() group by a, b)
) engine = CnchMergeTree() order by a settings index_granularity = 1024;

insert into t40037 select number % 100, (number + 37) % 100, number from system.numbers limit 100000;

set enable_optimizer = 1, optimizer_projection_support = 1;

-- p_agg is used
select a, b, count() as cnt from t40037 group by a, b order by a, b;
-- p_agg is not used as 'c' is required
select a, b, count() as cnt from t40037 where c % 10 = 1 group by a, b order by a, b;

-- p_norm is used
select sum(c) from t40037 where b = 1;
-- p_norm is not used as no PREWHER/WHERE exists
select sum(c) from t40037;

-- p_agg is used
select count() as cnt from t40037 where a = 1;
-- p_norm is used as normal projection takes higher priority
select count() as cnt from t40037 where b = 1;
