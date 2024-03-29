drop table if exists t_00818;
drop table if exists s_00818;
create table t_00818(a Nullable(Int64), b Nullable(Int64), c Nullable(String)) engine = CnchMergeTree order by a settings allow_nullable_key = 1;
create table s_00818(a Nullable(Int64), b Nullable(Int64), c Nullable(String)) engine = CnchMergeTree order by a settings allow_nullable_key = 1;
insert into t_00818 values(1,1,'a'), (2,2,'b');
insert into s_00818 values(1,1,'a');
set enable_predicate_pushdown_rewrite=0;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a order by t_00818.a;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a and t_00818.a = s_00818.b order by t_00818.a;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a where s_00818.a = 1 order by t_00818.a;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a and t_00818.a = s_00818.a order by t_00818.a;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a and t_00818.b = s_00818.a order by t_00818.a;
drop table t_00818;
drop table s_00818;
