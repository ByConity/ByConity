set enable_optimizer=1;

DROP TABLE IF EXISTS t46009;
CREATE TABLE t46009(a Int, b Int) Engine = CnchMergeTree() order by a;

explain syntax select t46009.a, count(a) from t46009 join t46009 as t2 on t46009.a=t2.b where t46009.a < 10 group by t46009.a;

explain syntax select t46009.a, count(a) from t46009 join t46009 as t2 on t46009.a=t2.b where t46009.a < 10 and (t46009.a in ((select b from t46009) as t3)) group by t46009.a;

explain syntax select name from system.settings settings enable_optimizer=1;

DROP TABLE IF EXISTS t46009;
