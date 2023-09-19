use test;
DROP TABLE IF EXISTS analysis;
DROP TABLE IF EXISTS analysis2;
CREATE TABLE analysis(a UInt32, b UInt32) ENGINE = CnchMergeTree() partition by a order by a;
CREATE TABLE analysis2 (a UInt32, b UInt32, c Nullable(UInt64)) ENGINE = CnchMergeTree() partition by a order by a;

explain analysis select '1';

explain analysis select 1+1;

explain analysis select * from analysis;

explain analysis select t1.a, t2.b from analysis t1 join analysis2 t2 on t1.a=t2.a settings enable_optimizer=1;

explain analysis select t1.a, t2.b, t2.a+1 from analysis t1 join analysis2 t2 on t1.a=t2.a;

explain analysis insert into analysis select t1.a as a, t2.b as b from analysis t1 join analysis2 t2 on t1.a=t2.a settings enable_optimizer=1;

explain analysis insert into analysis2 (a, b) select t1.a as a, t2.b as b from analysis t1 join analysis2 t2 on t1.a=t2.a settings enable_optimizer=1;

explain analysis select * from analysis order by a limit 3 with ties;

explain analysis insert into analysis select t1.a as a, t2.b as b from analysis t1  ANY FULL JOIN analysis2 t2 using(a);

DROP TABLE IF EXISTS analysis;
DROP TABLE IF EXISTS analysis2;
