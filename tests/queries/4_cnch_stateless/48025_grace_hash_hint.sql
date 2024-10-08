DROP TABLE IF EXISTS t48025;
DROP TABLE IF EXISTS t480252;

CREATE TABLE t48025 (x UInt32, s String) engine = CnchMergeTree() order by x;
CREATE TABLE t480252 (x UInt32, s String) engine = CnchMergeTree() order by x;

INSERT INTO t48025 (x, s) VALUES (0, 'a1'), (1, 'a2'), (2, 'a3'), (3, 'a4'), (4, 'a5'), (2, 'a6');
INSERT INTO t480252 (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (8, 'b5'), (5, 'b6');

SET enable_optimizer = 1;

select
    *
from t48025 t1 join t480252 t2 on t1.x=t2.x
order by t1.x;

select /*+ use_grace_hash(t1, t2) */
    *
from t48025 t1 join t480252 t2 on t1.x=t2.x
order by t1.x;

explain select /*+ use_grace_hash(t1, t2) */
    *
from t48025 t1 join t480252 t2 on t1.x=t2.x
order by t1.x;

select
    *
from t48025 t1 join t480252 t2 on t1.x=t2.x join t48025 t3 on t1.x=t3.x
order by t1.x;

select /*+ use_grace_hash(t1, t3) */
    *
from t48025 t1 join t480252 t2 on t1.x=t2.x join t48025 t3 on t1.x=t3.x
order by t1.x;

explain select /*+ use_grace_hash(t1, t3) */
    *
from t48025 t1 join t480252 t2 on t1.x=t2.x join t48025 t3 on t1.x=t3.x
order by t1.x;


DROP TABLE IF EXISTS t48025;
DROP TABLE IF EXISTS t480252;
