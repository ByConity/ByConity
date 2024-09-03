-- The include statement below is a temp one for tests that are yet to
--be ported to run with InnoDB,
--but needs to be kept for tests that would need MyISAM in future.
--source include/force_myisam_default.inc


-- Testing of CASE
--

--disable_warnings
drop table if exists t1, t2;
--enable_warnings

-- SET sql_mode = 'NO_ENGINE_SUBSTITUTION';

select CASE 'b' when 'a' then 1 when 'b' then 2 END;
select CASE 'c' when 'a' then 1 when 'b' then 2 END;
select CASE 'c' when 'a' then 1 when 'b' then 2 ELSE 3 END;
select CASE 'b' when 'a' then 1 when 'B' then 2 WHEN 'b' then 'ok' END;
select CASE 'b' when 'a' then 1 when binary 'B' then 2 WHEN 'b' then 'ok' END;
select CASE concat('a','b') when concat('ab','') then 'a' when 'b' then 'b' end;
select CASE when 1=0 then 'true' else 'false' END;
select CASE 1 when 1 then 'one' WHEN 2 then 'two' ELSE 'more' END;
explain extended select CASE 1 when 1 then 'one' WHEN 2 then 'two' ELSE 'more' END;
select CASE 2.0 when 1 then 'one' WHEN 2.0 then 'two' ELSE 'more' END;
select (CASE 'two' when 'one' then '1' WHEN 'two' then '2' END) | 0;
select (CASE 'two' when 'one' then 1.00 WHEN 'two' then 2.00 END) +0.0;
select case 1/0 when 'a' then 'true' else 'false' END;
select case 1/0 when 'a' then 'true' END;
select (case 1/0 when 'a' then 'true' END) | 0;
select (case 1/0 when 'a' then 'true' END) + 0.0;
select case when 1>0 then 'TRUE' else 'FALSE' END;
select case when 1<0 then 'TRUE' else 'FALSE' END;

--
-- Test bug when using GROUP BY on CASE
--
create table t1 (a int);
insert into t1 values(1),(2),(3),(4);
select case a when 1 then 2 when 2 then 3 else 0 end as fcase, count(*) from t1 group by fcase;
explain extended select case a when 1 then 2 when 2 then 3 else 0 end as fcase, count(*) from t1 group by fcase;
select case a when 1 then 'one' when 2 then 'two' else 'nothing' end as fcase, count(*) from t1 group by fcase;
drop table t1;

--
-- Test MAX(CASE ... ) that can return null
--

create table t1 (row int not null, col int not null, val varchar(255) not null);
insert into t1 values (1,1,'orange'),(1,2,'large'),(2,1,'yellow'),(2,2,'medium'),(3,1,'green'),(3,2,'small');
select max(case col when 1 then val else null end) as color from t1 group by row;
drop table t1;

-- SET NAMES latin1;

--
-- CASE and argument types/collations aggregation into result 
--
CREATE TABLE t1 SELECT 
 CASE WHEN 1 THEN 'a' ELSE 'a' END AS c1,
 CASE WHEN 1 THEN 'a' ELSE 'a' END AS c2,
 CASE WHEN 1 THEN 'a' ELSE  1  END AS c3,
 CASE WHEN 1 THEN  1  ELSE 'a' END AS c4,
 CASE WHEN 1 THEN 'a' ELSE 1.0 END AS c5,
 CASE WHEN 1 THEN 1.0 ELSE 'a' END AS c6,
 CASE WHEN 1 THEN  1  ELSE 1.0 END AS c7,
 CASE WHEN 1 THEN 1.0 ELSE  1  END AS c8,
 CASE WHEN 1 THEN 1.0 END AS c9,
 CASE WHEN 1 THEN 0.1e1 else 0.1 END AS c10,
 CASE WHEN 1 THEN 0.1e1 else 1 END AS c11,
 CASE WHEN 1 THEN 0.1e1 else '1' END AS c12
;
SHOW CREATE TABLE t1;
DROP TABLE t1;
--
-- COALESCE is a CASE abbrevation:
--
-- COALESCE(v1,v2) == CASE WHEN v1 IS NOT NULL THEN v1 ELSE v2 END
--
-- COALESCE(V1, V2, . . . ,Vn ) =  
--     CASE WHEN V1 IS NOT NULL THEN V1 ELSE COALESCE (V2, . . . ,Vn) END
--
-- Check COALESCE argument types aggregation

--error 1267
CREATE TABLE t1 SELECT COALESCE('a','a');
--error 1267
CREATE TABLE t1 SELECT COALESCE('a' COLLATE latin1_swedish_ci,'b' COLLATE latin1_bin);
CREATE TABLE t1 SELECT 
 COALESCE(1), COALESCE(1.0),COALESCE('a'),
 COALESCE(1,1.0), COALESCE(1,'1'),COALESCE(1.1,'1'),
 COALESCE('a' COLLATE latin1_bin,'b');
explain extended SELECT 
 COALESCE(1), COALESCE(1.0),COALESCE('a'),
 COALESCE(1,1.0), COALESCE(1,'1'),COALESCE(1.1,'1'),
 COALESCE('a' COLLATE latin1_bin,'b');
SHOW CREATE TABLE t1;
DROP TABLE t1;

--error 1267
CREATE TABLE t1 SELECT IFNULL('a' COLLATE latin1_swedish_ci, 'b' COLLATE latin1_bin);

-- Test for BUG#10151
SELECT 'case+union+test'
UNION 
SELECT CASE LOWER('1') WHEN LOWER('2') THEN 'BUG' ELSE 'nobug' END;

SELECT CASE LOWER('1') WHEN LOWER('2') THEN 'BUG' ELSE 'nobug' END;

SELECT 'case+union+test'
UNION 
SELECT CASE '1' WHEN '2' THEN 'BUG' ELSE 'nobug' END;

--
-- Bug #17896: problem with MIN(CASE...)
--

create table t1(a float, b int default 3);
insert into t1 (a) values (2), (11), (8);
select min(a), min(case when 1=1 then a else NULL end),
  min(case when 1!=1 then NULL else a end) 
from t1 where b=3 group by b;
drop table t1;


--
-- Tests for bug #9939: conversion of the arguments for COALESCE and IFNULL
--

CREATE TABLE t1 (EMPNUM INT);
INSERT INTO t1 VALUES (0), (2);
CREATE TABLE t2 (EMPNUM DECIMAL (4, 2));
INSERT INTO t2 VALUES (0.0), (9.0);

SELECT COALESCE(t2.EMPNUM,t1.EMPNUM) AS CEMPNUM,
               t1.EMPNUM AS EMPMUM1, t2.EMPNUM AS EMPNUM2
  FROM t1 LEFT JOIN t2 ON t1.EMPNUM=t2.EMPNUM;

SELECT IFNULL(t2.EMPNUM,t1.EMPNUM) AS CEMPNUM,
               t1.EMPNUM AS EMPMUM1, t2.EMPNUM AS EMPNUM2
  FROM t1 LEFT JOIN t2 ON t1.EMPNUM=t2.EMPNUM;

DROP TABLE t1,t2;

--echo End of 4.1 tests

--
-- #30782: Truncated UNSIGNED BIGINT columns 
--
create table t1 (a int, b bigint unsigned);
create table t2 (c int);
insert into t1 (a, b) values (1,4572794622775114594), (2,18196094287899841997),
  (3,11120436154190595086);
insert into t2 (c) values (1), (2), (3);
select t1.a, (case t1.a when 0 then 0 else t1.b end) d from t1 
  join t2 on t1.a=t2.c order by d;
select t1.a, (case t1.a when 0 then 0 else t1.b end) d from t1 
  join t2 on t1.a=t2.c where b=11120436154190595086 order by d;
drop table t1, t2;

--echo End of 5.0 tests

--
-- Bug #11764313 57135: CRASH IN ITEM_FUNC_CASE::FIND_ITEM WITH CASE WHEN
-- ELSE CLAUSE
--

CREATE TABLE t1(a YEAR);
SELECT 1 FROM t1 WHERE a=1 AND CASE 1 WHEN a THEN 1 ELSE 1 END;
DROP TABLE t1;

-- SET sql_mode = default;

--echo #
--echo # Bug#19875294 ASSERTION `SRC' FAILED IN MY_STRNXFRM_UNICODE
--echo #              (SIG 6 -STRINGS/CTYPE-UTF8.C:5151)
--echo #

-- set @@sql_mode='';
CREATE TABLE t1(c1 SET('','')) engine=innodb;
INSERT INTO t1 VALUES(990101.102);
SELECT COALESCE(c1)FROM t1 ORDER BY 1;
DROP TABLE t1;
-- set @@sql_mode=default;
