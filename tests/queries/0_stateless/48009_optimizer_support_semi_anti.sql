use test;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t1_local;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t2_local;

CREATE TABLE t1_local (x UInt32, s String) engine = MergeTree() order by x;
create table t1 as t1_local engine = Distributed(test_shard_localhost, currentDatabase(), t1_local);
CREATE TABLE t2_local (x UInt32, s String) engine = MergeTree() order by x;
create table t2 as t2_local engine = Distributed(test_shard_localhost, currentDatabase(), t2_local);

INSERT INTO t1 (x, s) VALUES (0, 'a1'), (1, 'a2'), (2, 'a3'), (3, 'a4'), (4, 'a5'), (2, 'a6');
INSERT INTO t2 (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');

SET enable_optimizer = 1;
SET join_use_nulls = 0;
SET enable_optimizer_white_list = 0;

SELECT 'semi and anti join';
SELECT 'anti left';
SELECT t2.*, t1.* FROM t1 LEFT ANTI JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'anti right';
SELECT t1.*, t2.*  FROM t1 RIGHT ANTI JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'anti left';
SELECT t2.*, t1.*  FROM t1 ANTI LEFT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'anti right';
SELECT t1.*, t2.*  FROM t1 ANTI RIGHT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'semi left';
SELECT t1.*, t2.* FROM t1 LEFT SEMI JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'semi right';
SELECT t1.*, t2.* FROM t1 RIGHT SEMI JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'semi left';
SELECT t1.*, t2.* FROM t1 SEMI LEFT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

SELECT 'semi right';
SELECT t1.*, t2.* FROM t1 SEMI RIGHT JOIN t2 USING(x) ORDER BY t1.x, t2.x;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t1_local;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t2_local;
