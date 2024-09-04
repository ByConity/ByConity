drop table if exists t40093_push_join_t1;
drop table if exists t40093_push_join_t2;

create table t40093_push_join_t1(a Nullable(Int32), b Nullable(Int32)) engine = CnchMergeTree() order by tuple();

create table t40093_push_join_t2(a1 Nullable(Int32), b1 Nullable(Int32)) engine = CnchMergeTree() order by tuple();

select
    l.a, l.b, r.uniq_v
from t40093_push_join_t1 l
left join (
    select a, b, uniq(b1) uniq_v
    from (select distinct a, b from t40093_push_join_t1) t1
    left join t40093_push_join_t2 t2 on t1.a = t2.a1
    group by a, b
) r on l.a = r.a and l.b = r.b;

select
    l.a, l.b, r.sum_v
from t40093_push_join_t1 l
left join (
    select a, b, sum(b1) sum_v
    from (select distinct a, b from t40093_push_join_t1) t1
    left join t40093_push_join_t2 t2 on t1.a = t2.a1
    group by a, b
) r on l.a = r.a and l.b = r.b;

drop table if exists t40093_push_join_t1;
drop table if exists t40093_push_join_t2;
