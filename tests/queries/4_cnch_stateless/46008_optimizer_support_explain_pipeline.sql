set enable_optimizer=1;

DROP TABLE IF EXISTS test46008;
DROP TABLE IF EXISTS test460082;
CREATE TABLE test46008(a Int, b Int) Engine = CnchMergeTree() order by a;
INSERT INTO test46008 select number, number from numbers(5);
INSERT INTO test46008 select number, number from numbers(5,2);

set max_threads =1;
explain pipeline select * from test46008 format Null;
select * from test46008 order by a,b;

CREATE TABLE test460082(a Int, b Int) Engine = CnchMergeTree() order by a;
INSERT INTO test460082 select number, number from numbers(5);
INSERT INTO test46008 select number, number from numbers(5,2);

set max_threads =2;

explain pipeline select * from test460082 format Null;
explain pipeline select * from test46008 join test460082 on test46008.a=test460082.a where test46008.a < 10 format Null;

DROP TABLE IF EXISTS test46008;
DROP TABLE IF EXISTS test460082;
