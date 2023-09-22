use test;
set enable_optimizer=1;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(a Int, b Int) Engine = CnchMergeTree() order by a;

explain syntax select t1.a, count(a) from t1 join t1 as t2 on t1.a=t2.b where t1.a < 10 group by t1.a;

explain syntax select t1.a, count(a) from t1 join t1 as t2 on t1.a=t2.b where t1.a < 10 and (t1.a in ((select b from t1) as t3)) group by t1.a;

DROP TABLE IF EXISTS t1;
