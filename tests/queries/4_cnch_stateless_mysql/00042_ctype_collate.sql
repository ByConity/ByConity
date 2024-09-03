
--  #Get deafult engine value
--let $DEFAULT_ENGINE = `select @@global.default_storage_engine`

--disable_warnings
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
--enable_warnings

CREATE TABLE t1 (
  latin1_f CHAR(32) NOT NULL
);

--error 1253
CREATE TABLE t2 (
  latin1_f CHAR(32) NOT NULL
);


INSERT INTO t1 (latin1_f) VALUES ('A');
INSERT INTO t1 (latin1_f) VALUES ('a');

INSERT INTO t1 (latin1_f) VALUES ('AD');
INSERT INTO t1 (latin1_f) VALUES ('ad');

INSERT INTO t1 (latin1_f) VALUES ('AE');
INSERT INTO t1 (latin1_f) VALUES ('ae');

INSERT INTO t1 (latin1_f) VALUES ('AF');
INSERT INTO t1 (latin1_f) VALUES ('af');

INSERT INTO t1 (latin1_f) VALUES ('�');
INSERT INTO t1 (latin1_f) VALUES ('�');

INSERT INTO t1 (latin1_f) VALUES ('�');
INSERT INTO t1 (latin1_f) VALUES ('�');

INSERT INTO t1 (latin1_f) VALUES ('B');
INSERT INTO t1 (latin1_f) VALUES ('b');

INSERT INTO t1 (latin1_f) VALUES ('U');
INSERT INTO t1 (latin1_f) VALUES ('u');

INSERT INTO t1 (latin1_f) VALUES ('UE');
INSERT INTO t1 (latin1_f) VALUES ('ue');

INSERT INTO t1 (latin1_f) VALUES ('�');
INSERT INTO t1 (latin1_f) VALUES ('�');

INSERT INTO t1 (latin1_f) VALUES ('SS');
INSERT INTO t1 (latin1_f) VALUES ('ss');
INSERT INTO t1 (latin1_f) VALUES ('�');

INSERT INTO t1 (latin1_f) VALUES ('Y');
INSERT INTO t1 (latin1_f) VALUES ('y');

INSERT INTO t1 (latin1_f) VALUES ('Z');
INSERT INTO t1 (latin1_f) VALUES ('z');


-- ORDER BY
SELECT latin1_f FROM t1 ORDER BY latin1_f, hex(latin1_f);
-- GROUP BY
SELECT latin1_f,count(*) FROM t1 GROUP BY latin1_f;
-- DISTINCT
SELECT DISTINCT latin1_f                           FROM t1;

--
-- Check that SHOW displays COLLATE clause
--


--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
SHOW CREATE TABLE t1;
-- SHOW FIELDS FROM  t1;

--
-- Check SET CHARACTER SET
--

-- SET CHARACTER SET 'latin1';
SHOW VARIABLES LIKE 'character_set_client';
SELECT charset('a'),collation('a'),coercibility('a'),'a'='A';
explain extended SELECT charset('a'),collation('a'),coercibility('a'),'a'='A';

-- SET CHARACTER SET koi8r;
SHOW VARIABLES LIKE 'collation_client';
SELECT charset('a'),collation('a'),coercibility('a'),'a'='A';

--error 1115
-- SET CHARACTER SET 'DEFAULT';

DROP TABLE t1;

CREATE TABLE t1 
(s1 CHAR(5),
 s2 CHAR(5));
--error 1267
SELECT * FROM t1 WHERE s1 = s2;
DROP TABLE t1;


CREATE TABLE t1
(s1 CHAR(5),
 s2 CHAR(5),
 s3 CHAR(5));
INSERT INTO t1 VALUES ('a','A','A');
--error 1267
SELECT * FROM t1 WHERE s1 = s2;
SELECT * FROM t1 WHERE s1 = s3;
SELECT * FROM t1 WHERE s2 = s3;
DROP TABLE t1;


--
-- Test that optimizer doesn't use indexes with wrong collation
--
--
-- BUG#48447, Delivering too few records with indexes using collate syntax
--
create table t1 (a varchar(1));
insert into t1 values ('A'),('a'),('B'),('b'),('C'),('c');
select * from t1 where a > 'B';
select * from t1 where a <> 'B';
create index i on t1 (a);
select * from t1 where a > 'B';
select * from t1 where a <> 'B';
drop table t1;

-- SET NAMES latin1;
CREATE TABLE t1 
(s1 char(10),
 s2 char(10),
 KEY(s1),
 KEY(s2));

INSERT INTO t1 VALUES ('a','a');
INSERT INTO t1 VALUES ('b','b');
INSERT INTO t1 VALUES ('c','c');
INSERT INTO t1 VALUES ('d','d');
INSERT INTO t1 VALUES ('e','e');
INSERT INTO t1 VALUES ('f','f');
INSERT INTO t1 VALUES ('g','g');
INSERT INTO t1 VALUES ('h','h');
INSERT INTO t1 VALUES ('i','i');
INSERT INTO t1 VALUES ('j','j');

ANALYZE TABLE t1;
EXPLAIN SELECT * FROM t1 WHERE s1='a';
EXPLAIN SELECT * FROM t1 WHERE s2='a';
EXPLAIN SELECT * FROM t1 WHERE s1='a';
--replace_result 11 10 9.09 10.00
EXPLAIN SELECT * FROM t1 WHERE s2='a';

EXPLAIN SELECT * FROM t1 WHERE s1 BETWEEN 'a' AND 'b';
--replace_result 11 10
EXPLAIN SELECT * FROM t1 WHERE s2 BETWEEN 'a' AND 'b';

EXPLAIN SELECT * FROM t1 WHERE s1 IN  ('a','b');
--replace_result 11 10 18.18 20.00 3.31 4.00
EXPLAIN SELECT * FROM t1 WHERE s2 IN  ('a','b');

EXPLAIN SELECT * FROM t1 WHERE s1 LIKE 'a';
--replace_result 11 10
EXPLAIN SELECT * FROM t1 WHERE s2 LIKE 'a';

DROP TABLE t1;

-- End of 4.1 tests

--
-- Bug#29261: Sort order of the collation wasn't used when comparing trailing
--            spaces.
--
create table t1(f1 varchar(10), key(f1));
insert into t1 set f1=0x'3F3F9DC73F';
insert into t1 set f1=0x'3F3F1E563F';
insert into t1 set f1=0x'3F3F';
drop table t1;

--
-- Bug#29461: Sort order of the collation wasn't used when comparing characters
--            with the space character.
--
create table t1 (a varchar(2),key(a));
insert into t1 set a=0x4c20;
insert into t1 set a=0x6c;
insert into t1 set a=0x4c98;
drop table t1;

select concat('a','b','c');
-- SET sql_mode = default;

--echo # Bug#20425399: CAN'T USE COLLATE
CREATE TABLE t1(a TINYINT, b SMALLINT, c MEDIUMINT, d INT, e BIGINT);
CREATE TABLE t2(a DECIMAL(5,2));
CREATE TABLE t3(a FLOAT(5,2), b DOUBLE(5,2));
INSERT INTO t1 VALUES(1, 2, 3, 4, 100);
INSERT INTO t1 VALUES(2, 3, 4, 100, 1);
INSERT INTO t1 VALUES(3, 4, 100, 1, 2);
INSERT INTO t1 VALUES(4, 100, 1, 2, 3);
INSERT INTO t1 VALUES(100, 1, 2, 3, 4);
SELECT * FROM t1 ORDER BY a;
SELECT * FROM t1 ORDER BY b;
SELECT * FROM t1 ORDER BY c;
SELECT * FROM t1 ORDER BY d;
INSERT INTO t2 VALUES(1.01);
INSERT INTO t2 VALUES(2.99);
INSERT INTO t2 VALUES(100.49);
SELECT * FROM t2 ORDER BY a;
INSERT INTO t3 VALUES(1.01, 2.99);
INSERT INTO t3 VALUES(2.99, 100.49);
INSERT INTO t3 VALUES(100.49, 1.01);
SELECT * FROM t3 ORDER BY a;
SELECT * FROM t3 ORDER BY b;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
