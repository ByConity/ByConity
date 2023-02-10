drop table if exists t;
drop table if exists t1;

create table t(id UInt32) engine CnchMergeTree order by id as select 1;

create table t1(a Array(UInt32)) ENGINE = CnchMergeTree ORDER BY tuple() as select [1,2];

select count() from t array join (select a from t1) AS _a settings optimize_trivial_count_query=1;

drop table t;
drop table t1;
