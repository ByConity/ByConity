drop table if exists t;

create table t (x UInt8, id UInt8) ENGINE = CnchMergeTree() order by (id) settings enable_late_materialize = 1;
insert into t values (1, 1);

set enable_optimize_predicate_expression = 0;

-- select 1 from t as l join t as r on l.id = r.id prewhere l.x; /// optimizer does not support prewhere join
select 1 from t as l join t as r on l.id = r.id where l.x;
select 2 from t as l join t as r on l.id = r.id where r.x;
select 3 from t as l join t as r on l.id = r.id where l.x and r.x;
select 4 from t as l join t as r using id where l.x and r.x;

select 5 from t as l join t as r on l.id = r.id where l.x and r.x;
select 6 from t as l join t as r using id where l.x and r.x;

select 7 from t as l join t as r on l.id = r.id where l.x and r.x;
select 8 from t as l join t as r using id where l.x and r.x;
    
drop table t;
