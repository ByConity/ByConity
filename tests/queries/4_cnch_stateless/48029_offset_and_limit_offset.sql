DROP TABLE IF EXISTS t48029;
set enable_optimizer=1;

CREATE TABLE t48029 (a UInt32, b UInt32) ENGINE = CnchMergeTree() partition by a order by a;

insert into t48029 values(1,2)(2,3)(3,4);

SELECT a FROM t48029 ORDER BY a OFFSET 5;
SELECT a FROM t48029 ORDER BY a OFFSET 2;
select t1.a from t48029 t1 join t48029 t2 on t1.a=t2.a order by t1.a offset 2;

DROP TABLE IF EXISTS t48029;

drop table if exists limit_by;

create table limit_by(id Int, val Int) ENGINE = CnchMergeTree() partition by id order by id;

insert into limit_by values(1, 100), (1, 110), (1, 120), (1, 130), (2, 200), (2, 210), (2, 220), (3, 300);

select * from limit_by order by id, val limit 2, 2 by id;
select * from limit_by order by id, val limit 2 offset 1 by id;
select * from limit_by order by id, val limit 1, 2 by id limit 3;
select * from limit_by order by id, val limit 1, 2 by id limit 3 offset 1;

drop table limit_by;
