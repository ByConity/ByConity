use test;
DROP TABLE IF EXISTS t48028;
DROP TABLE IF EXISTS t480282;
set enable_optimizer=1;
CREATE TABLE t48028 (a UInt32, b UInt32) ENGINE = CnchMergeTree() partition by a order by a;

CREATE TABLE t480282 (a UInt32, b UInt32) ENGINE = CnchMergeTree() partition by a order by a;

insert into t48028 values(1,2)(2,3)(3,4);
insert into t480282 values(1,2)(2,3)(3,4);

explain json=1 select t1.a, t2.b from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

set cte_mode='SHARED';
explain json=1 with t1 as (select * from t48028) select t1.a, t2.b from t1 t1 join t1 t2 on t1.a=t2.a format Null;

explain distributed json=1 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

explain analyze json=1 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

explain analyze distributed json=1 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

DROP TABLE IF EXISTS t48028;
DROP TABLE IF EXISTS t480282;
