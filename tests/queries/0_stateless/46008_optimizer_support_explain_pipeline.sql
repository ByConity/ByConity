set enable_optimizer=1;

DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;
CREATE TABLE test1(a Int, b Int) Engine = MergeTree() order by a;
INSERT INTO test1 select number, number from numbers(5);
INSERT INTO test1 select number, number from numbers(5,2);

set max_threads =1;
explain pipeline select * from test1 ;
select * from test1;

CREATE TABLE test2(a Int, b Int) Engine = MergeTree() order by a;
INSERT INTO test2 select number, number from numbers(5);
INSERT INTO test1 select number, number from numbers(5,2);

set max_threads =2;

explain pipeline select * from test2;
explain pipeline select * from test1 join test2 on test1.a=test2.a where test1.a < 10;

DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;
