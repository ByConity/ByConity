-- # Check some special create statements.
-- #
-- # SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
--disable_warnings
drop table if exists t1,t2,t3,t4,t5;
drop database if exists mysqltest;
drop view if exists v1;
--enable_warnings

create table t1 (b char(0));
insert into t1 values (''),(null);
select * from t1;
drop table if exists t1;

create table t1 (b char(0) not null);
create table if not exists t1 (b char(0) not null);
insert into t1 values (''),(null);
select * from t1;
drop table t1;

create table t1 (a int not null auto_increment,primary key (a)) engine=heap;
drop table t1;

create table `a/a` (a int);
show create table `a/a`;
create table t1 like `a/a`;
drop table `a/a`;
drop table `t1`;
--error ER_TOO_LONG_IDENT
create table `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` (aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int);
--error 1059
create table a (`aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` int);

-- #
-- # Some wrong defaults, so these creates should fail too (Bug -- #5902)
-- #
create table t1 (a varchar(5) default 'abcde');
insert into t1 values();
select * from t1;
drop table t1;

-- #
-- # test of dummy table names
-- #

create table 1ea10 (1a20 int,1e int);
insert into 1ea10 values(1,1);
select 1ea10.1a20,1e+ 1e+10 from 1ea10;
drop table 1ea10;
create table t1 (t1.index int);
drop table t1;
-- # Test that we get warning for this
drop database if exists mysqltest;
create database mysqltest;
create table mysqltest.$test1 (a$1 int, $b int, c$ int);
insert into mysqltest.$test1 values (1,2,3);
select a$1, $b, c$ from mysqltest.$test1;
create table mysqltest.test2$ (a int);
drop table mysqltest.test2$;
drop database mysqltest;

-- #
-- # Test of CREATE ... SELECT with indexes
-- #

create table t1 (a int auto_increment not null primary key, B CHAR(20));
insert into t1 (b) values ('hello'),('my'),('world');
create table t2 (key (b)) select * from t1;
explain select * from t2 where b='world';
select * from t2 where b='world';
drop table t1,t2;

-- #
-- # Test types after CREATE ... SELECT
-- #

create table t1(x varchar(50) );
create table t2 select x from t1 where 1=2;
describe t1;
describe t2;
drop table t2;
create table t2 select now() as a , curtime() as b, curdate() as c , 1+1 as d , 1.0 + 1 as e , 33333333333333333 + 3 as f;
describe t2;
drop table t2;
create table t2 select CAST('2001-12-29' AS DATE) as d, CAST('20:45:11' AS TIME) as t, CAST('2001-12-29  20:45:11' AS DATETIME) as dt;
describe t2;
drop table t1,t2;

-- #
-- # Test of CREATE ... SELECT with duplicate fields
-- #

create table t1 (a tinyint);
create table t2 (a int) select * from t1;                        
describe t1;
describe t2;     
drop table if exists t1,t2;

-- #
-- # Test of primary key with 32 index
-- #

create table t1 (a int not null, b int, primary key(a), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b), key (b));
show create table t1;
drop table t1;
create table t1 select if(1,'1','0'), month('2002-08-02');
drop table t1;
create table t1 select if('2002'='2002','Y','N');
select * from t1;
drop table if exists t1;

-- #
-- # Test default table type
-- #
-- # SET SESSION default_storage_engine='heap';
-- SELECT @@default_storage_engine;
CREATE TABLE t1 (a int not null);
show create table t1;
drop table t1;
--error 1286
-- # SET SESSION default_storage_engine='gemini';
-- SELECT @@default_storage_engine;
CREATE TABLE t1 (a int not null);
show create table t1;
-- # SET SESSION default_storage_engine=default;
drop table t1;


-- #
-- # ISO requires that primary keys are implicitly NOT NULL
-- #
create table t1 ( k1 varchar(2), k2 int, primary key(k1,k2));
insert into t1 values ('a', 1), ('b', 2);
--error 1048
insert into t1 values ('c', NULL);
--error 1048
insert into t1 values (NULL, 3);
--error 1048
insert into t1 values (NULL, NULL);
drop table t1;

-- #
-- # Bug -- # 801
-- #

create table t1 select x'4132';
drop table t1;

-- #
-- # bug -- #1434
-- #

create table t1 select 1,2,3;
create table if not exists t1 select 1,2;
create table if not exists t1 select 1,2,3,4;
create table if not exists t1 select 1;
select * from t1;
drop table t1;

-- #
-- # Test create table if not exists with duplicate key error
-- #

-- flush status;
create table t1 (a int not null, b int, primary key (a));
insert into t1 values (1,1);
create table if not exists t1 select 2;
select * from t1;
create table if not exists t1 select 3 as `a`,4 as `b`;
-- show warnings;
show status like 'Opened_tables';
select * from t1;
drop table t1;

-- #
-- # Test create with foreign keys
-- #

create table t1 (a int, key(a));
create table t2 (b int, foreign key(b) references t1(a), key(b));
drop table if exists t2,t1;

-- #
-- # Test for CREATE TABLE .. LIKE ..
-- #

create table t1(id int not null, name char(20));
insert into t1 values(10,'mysql'),(20,'monty- the creator');
create table t2(id int not null);
insert into t2 values(10),(20);
create table t3 like t1;
show create table t3;
select * from t3;
--replace_result InnoDB TMP_TABLE_ENGINE MyISAM TMP_TABLE_ENGINE 
show create table t3;
select * from t3;
drop table t2, t3;
create database mysqltest;
create table mysqltest.t3 like t1;
create temporary table t3 like mysqltest.t3;
--replace_result InnoDB TMP_TABLE_ENGINE MyISAM TMP_TABLE_ENGINE 
show create table t3;
create table t2 like t3;
show create table t2;
select * from t2;
create table t3 like t1;
drop table t1, t2, t3;
drop table t3;
drop database mysqltest;

-- #
-- # Test default table type
-- #
-- # SET SESSION default_storage_engine='heap';
-- SELECT @@default_storage_engine;
CREATE TABLE t1 (a int not null);
show create table t1;
drop table t1;
--error 1286
-- # SET SESSION default_storage_engine='gemini';
-- SELECT @@default_storage_engine;
CREATE TABLE t1 (a int not null);
show create table t1;
-- # SET SESSION default_storage_engine=default;
drop table t1;

-- #
-- # Test types of data for create select with functions
-- #

create table t1(a int,b int,c int unsigned,d date,e char,f datetime,g time,h blob);
insert into t1(a)values(1);
insert into t1(a,b,c,d,e,f,g,h)
values(2,-2,2,'1825-12-14','a','2003-1-1 3:2:1','4:3:2','binary data');
select * from t1;
select a, 
    ifnull(b,cast(-7 as signed)) as b, 
    ifnull(c,cast(7 as unsigned)) as c, 
    ifnull(d,cast('2000-01-01' as date)) as d, 
    ifnull(e,cast('b' as char)) as e,
    ifnull(f,cast('2000-01-01' as datetime)) as f, 
    ifnull(g,cast('5:4:3' as time)) as g,
    ifnull(h,cast('yet another binary data' as binary)) as h,
    addtime(cast('1:0:0' as time),cast('1:0:0' as time)) as dd 
from t1;

create table t2
select
    a, 
    ifnull(b,cast(-7                        as signed))   as b,
    ifnull(c,cast(7                         as unsigned)) as c,
    ifnull(d,cast('2000-01-01'              as date))     as d,
    ifnull(e,cast('b'                       as char))     as e,
    ifnull(f,cast('2000-01-01'              as datetime)) as f,
    ifnull(g,cast('5:4:3'                   as time))     as g,
    ifnull(h,cast('yet another binary data' as binary))   as h,
    addtime(cast('1:0:0' as time),cast('1:0:0' as time))  as dd
from t1;
explain t2;
select * from t2;
drop table t1, t2;

create table t1 (a tinyint, b smallint, c mediumint, d int, e bigint, f float(3,2), g double(4,3), h decimal(5,4), i year, j date, k timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, l datetime, m enum('a','b'), n set('a','b'), o char(10));
create table t2 select ifnull(a,a), ifnull(b,b), ifnull(c,c), ifnull(d,d), ifnull(e,e), ifnull(f,f), ifnull(g,g), ifnull(h,h), ifnull(i,i), ifnull(j,j), ifnull(k,k), ifnull(l,l), ifnull(m,m), ifnull(n,n), ifnull(o,o) from t1;
show create table t2;
drop table t1,t2;

-- #
-- # Test of default()
-- #
create table t1(str varchar(10) default 'def',strnull varchar(10),intg int default '10',rel double default '3.14');
insert into t1 values ('','',0,0.0);
describe t1;
create table t2 select default(str) as str, default(strnull) as strnull, default(intg) as intg, default(rel) as rel from t1;
describe t2;
drop table t1, t2;

-- #
-- # Bug -- #2075
-- #

create table t1(name varchar(10), age smallint default -1);
describe t1;
create table t2(name varchar(10), age smallint default - 1);
describe t2;
drop table t1, t2;

-- #
-- # test for bug -- #1427 'enum allows duplicate values in the list'
-- #

create table t1(cenum enum('a'), cset set('b'));
create table t2(cenum enum('a','a'), cset set('b','b'));
create table t3(cenum enum('a','A','a','c','c'), cset set('b','B','b','d','d'));
drop table t1, t2, t3;

-- #
-- # Test for Bug 856 'Naming a key 'Primary' causes trouble'
-- #

create table t1 (`primary` int, index(`primary`));
show create table t1;
create table t2 (`PRIMARY` int, index(`PRIMARY`));
show create table t2;

create table t3 (a int);
--error 1280
alter table t3 add index `primary` (a);
--error 1280
alter table t3 add index `PRIMARY` (a);

create table t4 (`primary` int);
alter table t4 add index(`primary`);
show create table t4;
create table t5 (`PRIMARY` int);
alter table t5 add index(`PRIMARY`);
show create table t5;

drop table t1, t2, t3, t4, t5;

-- #
-- # bug -- #3266 TEXT in CREATE TABLE SELECT
-- #

CREATE TABLE t1(id varchar(10) NOT NULL PRIMARY KEY, dsc longtext);
INSERT INTO t1 VALUES ('5000000001', NULL),('5000000003', 'Test'),('5000000004', NULL);
CREATE TABLE t2(id varchar(15) NOT NULL, proc varchar(100) NOT NULL, runID varchar(16) NOT NULL, start datetime NOT NULL, PRIMARY KEY  (id,proc,runID,start));

INSERT INTO t2 VALUES ('5000000001', 'proc01', '20031029090650', '2003-10-29 13:38:40'),('5000000001', 'proc02', '20031029090650', '2003-10-29 13:38:51'),('5000000001', 'proc03', '20031029090650', '2003-10-29 13:38:11'),('5000000002', 'proc09', '20031024013310', '2003-10-24 01:33:11'),('5000000002', 'proc09', '20031024153537', '2003-10-24 15:36:04'),('5000000004', 'proc01', '20031024013641', '2003-10-24 01:37:29'),('5000000004', 'proc02', '20031024013641', '2003-10-24 01:37:39');

CREATE TABLE t3  SELECT t1.dsc,COUNT(DISTINCT t2.id) AS countOfRuns  FROM t1 LEFT JOIN t2 ON (t1.id=t2.id) GROUP BY t1.id;
SELECT * FROM t3;
drop table t1, t2, t3;

-- #
-- # Bug-- #9666: Can't use 'DEFAULT FALSE' for column of type bool
-- #
create table t1 (b bool not null default false);
create table t2 (b bool not null default true);
insert into t1 values ();
insert into t2 values ();
select * from t1;
select * from t2;
drop table t1,t2;

-- #
-- # Bug -- #12537: UNION produces longtext instead of varchar
-- #
CREATE TABLE t1 (f1 VARCHAR(255) CHARACTER SET utf8);
CREATE TABLE t2 AS SELECT LEFT(f1,171) AS f2 FROM t1 UNION SELECT LEFT(f1,171) AS f2 FROM t1;
DESC t2;
DROP TABLE t1,t2;

-- #
-- # Bug-- #12913 Simple SQL can crash server or connection
-- #
CREATE TABLE t12913 (f1 ENUM ('a','b')) AS SELECT 'a' AS f1;
SELECT * FROM t12913;
DROP TABLE t12913;

-- #
-- # Bug -- #6859: Bogus error message on attempt to CREATE TABLE t LIKE view
-- #
create database mysqltest;
use mysqltest;
create view v1 as select 'foo' from dual;
--error 1347
create table t1 like v1;
drop view v1;
drop database mysqltest;
-- # Bug -- #6008 MySQL does not create warnings when
-- # creating database and using IF NOT EXISTS
-- #
create database mysqltest;
create database if not exists mysqltest character set latin2;
show create database mysqltest;
drop database mysqltest;
use test;
create table t1 (a int);
create table if not exists t1 (a int);
drop table t1;

-- # BUG-- #14139
create table t1 (
  a varchar(112) charset utf8 collate utf8_bin not null,
  primary key (a)
) select 'test' as a ;
-- #--warning 1364
show create table t1;
drop table t1;

-- #
-- # BUG-- #14480: assert failure in CREATE ... SELECT because of wrong
-- #            calculation of number of NULLs.
-- #
CREATE TABLE t2 (
  a int(11) default NULL
);
insert into t2 values(111);

-- #--warning 1364
create table t1 ( 
  a varchar(12) charset utf8 collate utf8_bin not null, 
  b int not null, primary key (a)
) select a, 1 as b from t2 ;
show create table t1;
drop table t1;

-- #--warning 1364
create table t1 ( 
  a varchar(12) charset utf8 collate utf8_bin not null, 
  b int not null, primary key (a)
) select a, 1 as c from t2 ;
show create table t1;
drop table t1;

-- #--warning 1364
create table t1 ( 
  a varchar(12) charset utf8 collate utf8_bin not null, 
  b int null, primary key (a)
) select a, 1 as c from t2 ;
show create table t1;
drop table t1;

-- #--warning 1364
create table t1 ( 
  a varchar(12) charset utf8 collate utf8_bin not null,
  b int not null, primary key (a)
) select 'a' as a , 1 as b from t2 ;
show create table t1;
drop table t1;

-- #--warning 1364
create table t1 ( 
  a varchar(12) charset utf8 collate utf8_bin,
  b int not null, primary key (a)
) select 'a' as a , 1 as b from t2 ;
show create table t1;
drop table t1, t2;

create table t1 ( 
  a1 int not null,
  a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int, a9 int
);
insert into t1 values (1,1,1, 1,1,1, 1,1,1);

-- #--warning 1364
create table t2 ( 
  a1 varchar(12) charset utf8 collate utf8_bin not null,
  a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int, a9 int,
  primary key (a1)
) select a1,a2,a3,a4,a5,a6,a7,a8,a9 from t1 ;
drop table t2;

-- #--warning 1364
create table t2 ( 
  a1 varchar(12) charset utf8 collate utf8_bin,
  a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int, a9 int
) select a1,a2,a3,a4,a5,a6,a7,a8,a9 from t1;

drop table t1, t2;
-- #--warning 1364
create table t1 ( 
  a1 int, a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int, a9 int
);
insert into t1 values (1,1,1, 1,1,1, 1,1,1);

-- #--warning 1364
create table t2 ( 
  a1 varchar(12) charset utf8 collate utf8_bin not null,
  a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int, a9 int,
  primary key (a1)
) select a1,a2,a3,a4,a5,a6,a7,a8,a9 from t1 ;

-- # Test the default value
drop table t2;

create table t2 ( a int default 3, b int default 3)
  select a1,a2 from t1;
show create table t2;

drop table t1, t2;


-- # End of 4.1 tests

-- #
-- # Tests for errors happening at various stages of CREATE TABLES ... SELECT
-- #
-- # (Also checks that it behaves atomically in the sense that in case
-- #  of error it is automatically dropped if it has not existed before.)
-- #
-- # Error in select_create::prepare() which is not related to table creation
create table t1 (a int);
create table if not exists t1 select 1 as a, 2 as b;
drop table t1;
-- # Finally error which happens during insert
--error ER_DUP_ENTRY
create table t1 (primary key (a)) (select 1 as a) union all (select 1 as a);
create table if not exists t1 select 1 as i;
select * from t1;
-- # After WL-- #5370, it just generates a warning that the table already exists.
create table if not exists t1 select * from t1;
select * from t1;
drop table t1;

-- # Base vs temporary tables dillema (a.k.a. bug-- #24508 'Inconsistent
-- # results of CREATE TABLE ... SELECT when temporary table exists').
-- # In this situation we either have to create non-temporary table and
-- # insert data in it or insert data in temporary table without creation of
-- # permanent table. After patch for Bug-- #47418, we create the base table and
-- # instert data into it, even though a temporary table exists with the same
-- # name.
create temporary table t1 (j int);
create table if not exists t1 select 1;
select * from t1;
drop temporary table t1;
select * from t1;
drop table t1;

-- #
-- # Bug-- #21772: can not name a column 'upgrade' when create a table
-- #
create table t1 (upgrade int);
drop table t1;


--echo
--echo Bug -- #26104 Bug on foreign key class constructor
--echo
--echo Check that ref_columns is initalized correctly in the constructor
--echo and semantic checks in mysql_prepare_table work.
--echo
--echo We do not need a storage engine that supports foreign keys
--echo for this test, as the checks are purely syntax-based, and the
--echo syntax is supported for all engines.
--echo
--disable_warnings
drop table if exists t1,t2;
--enable_warnings

create table t1(a int not null, b int not null, primary key (a, b));
--error ER_WRONG_FK_DEF
create table t2(a int not null, b int not null, c int not null, primary key (a),
foreign key fk_bug26104 (b,c) references t1(a));
drop table t1;

-- #
-- # Bug-- #15130:CREATE .. SELECT was denied to use advantages of the SQL_BIG_RESULT.
-- #
create table t1(f1 int,f2 int);
insert into t1 value(1,1),(1,2),(1,3),(2,1),(2,2),(2,3);
-- flush status;
create table t2 select sql_big_result f1,count(f2) from t1 group by f1;
-- show status like 'handler_read%';
drop table t1,t2;

-- #
-- # Bug -- #25162: Backing up DB from 5.1 adds 'USING BTREE' to KEYs on table creates
-- #

-- # Show that the old syntax for index type is supported
CREATE TABLE t1(c1 VARCHAR(33), KEY USING BTREE (c1));
DROP TABLE t1;

-- # Show that the new syntax for index type is supported
CREATE TABLE t1(c1 VARCHAR(33), KEY (c1) USING BTREE);
DROP TABLE t1;

-- # Show that in case of multiple index type definitions, the last one takes 
-- # precedence

CREATE TABLE t1(c1 VARCHAR(33), KEY USING BTREE (c1) USING HASH) ENGINE=MEMORY;
-- SHOW INDEX FROM t1;
DROP TABLE t1;

CREATE TABLE t1(c1 VARCHAR(33), KEY USING HASH (c1) USING BTREE) ENGINE=MEMORY;
-- SHOW INDEX FROM t1;
DROP TABLE t1;

-- #
-- # Bug-- #38821: Assert table->auto_increment_field_not_null failed in open_table()
-- #
CREATE TABLE t1 (a INTEGER AUTO_INCREMENT PRIMARY KEY, b INTEGER NOT NULL);
INSERT IGNORE INTO t1 (b) VALUES (5);

CREATE TABLE IF NOT EXISTS t2 (a INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY)
  SELECT a FROM t1;
--error 1062
INSERT INTO t2 SELECT a FROM t1;
--error 1062
INSERT INTO t2 SELECT a FROM t1;

DROP TABLE t1, t2;

--echo -- #
--echo -- # BUG-- #46384 - mysqld segfault when trying to create table with same 
--echo -- #             name as existing view
--echo -- #

CREATE TABLE t1 (a INT);
CREATE TABLE t2 (a INT);

INSERT INTO t1 VALUES (1),(2),(3);
INSERT INTO t2 VALUES (1),(2),(3);

CREATE VIEW v1 AS SELECT t1.a FROM t1, t2;
--error ER_TABLE_EXISTS_ERROR
CREATE TABLE v1 AS SELECT * FROM t1;

DROP VIEW v1;
DROP TABLE t1,t2;

--echo End of 5.0 tests

-- #
-- # Test of behaviour with CREATE ... SELECT
-- #

CREATE TABLE t1 (a int, b int);
insert into t1 values (1,1),(1,2);
--error ER_DUP_ENTRY
CREATE TABLE t2 (primary key (a)) select * from t1;
-- # This should give warning
drop table if exists t2;
--error ER_DUP_ENTRY
CREATE TEMPORARY TABLE t2 (primary key (a)) select * from t1;
-- # This should give warning
drop table if exists t2;
CREATE TABLE t2 (a int, b int, primary key (a));
--error ER_DUP_ENTRY
INSERT INTO t2 select * from t1;
SELECT * from t2;
TRUNCATE table t2;
--error ER_DUP_ENTRY
INSERT INTO t2 select * from t1;
SELECT * from t2;
drop table t2;

CREATE TEMPORARY TABLE t2 (a int, b int, primary key (a)) ENGINE=InnoDB;
--error ER_DUP_ENTRY
INSERT INTO t2 SELECT * FROM t1;
SELECT * from t2;
drop table t1,t2;

-- #
-- # Bug-- #21432 Database/Table name limited to 64 bytes, not chars, problems with multi-byte
-- #
-- # set names utf8;

create database имя_базы_в_кодировке_утф8_длиной_больше_чем_45;
use имя_базы_в_кодировке_утф8_длиной_больше_чем_45;
show databases;
use test;

select SCHEMA_NAME from information_schema.schemata
where schema_name='имя_базы_в_кодировке_утф8_длиной_больше_чем_45';

drop database имя_базы_в_кодировке_утф8_длиной_больше_чем_45;
create table имя_таблицы_в_кодировке_утф8_длиной_больше_чем_48
(
  имя_поля_в_кодировке_утф8_длиной_больше_чем_45 int,
  index имя_индекса_в_кодировке_утф8_длиной_больше_чем_48 (имя_поля_в_кодировке_утф8_длиной_больше_чем_45)
);

create view имя_вью_кодировке_утф8_длиной_больше_чем_42 as
select имя_поля_в_кодировке_утф8_длиной_больше_чем_45
from имя_таблицы_в_кодировке_утф8_длиной_больше_чем_48;

-- # database, table, field, key, view
select * from имя_таблицы_в_кодировке_утф8_длиной_больше_чем_48;

select TABLE_NAME from information_schema.tables where
table_schema='test';

select COLUMN_NAME from information_schema.columns where
table_schema='test';

select INDEX_NAME from information_schema.statistics where
table_schema='test';

select TABLE_NAME from information_schema.views where
table_schema='test';

show create table имя_таблицы_в_кодировке_утф8_длиной_больше_чем_48;
show create view имя_вью_кодировке_утф8_длиной_больше_чем_42;

drop view имя_вью_кодировке_утф8_длиной_больше_чем_42;
drop table имя_таблицы_в_кодировке_утф8_длиной_больше_чем_48;
-- # set names default;

-- #
-- # Bug-- #25629 CREATE TABLE LIKE does not work with INFORMATION_SCHEMA
-- #
create table t1 like information_schema.processlist;
--replace_result InnoDB TMP_TABLE_ENGINE MyISAM TMP_TABLE_ENGINE 
show create table t1;
drop table t1;
create temporary table t1 like information_schema.processlist;
--replace_result InnoDB TMP_TABLE_ENGINE MyISAM TMP_TABLE_ENGINE 
show create table t1;
drop table t1;
create table t1 like information_schema.character_sets;
show create table t1;
drop table t1;

-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #

--echo
--echo -- # --
--echo -- # -- Bug-- #21380: DEFAULT definition not always transfered by CREATE
--echo -- # -- TABLE/SELECT to the new table.
--echo -- # --
--echo


--disable_warnings
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
--enable_warnings

--echo

CREATE TABLE t1(
  c1 INT DEFAULT 12 COMMENT 'column1',
  c2 INT NULL COMMENT 'column2',
  c3 INT NOT NULL COMMENT 'column3',
  c4 VARCHAR(255) CHARACTER SET utf8 NOT NULL DEFAULT 'a',
  c5 VARCHAR(255) COLLATE utf8_unicode_ci NULL DEFAULT 'b',
  c6 VARCHAR(255))
  COLLATE latin1_bin;

--echo

SHOW CREATE TABLE t1;

--echo

CREATE TABLE t2 AS SELECT * FROM t1;

--echo

SHOW CREATE TABLE t2;

--echo

DROP TABLE t2;
DROP TABLE t1;

--echo
--echo -- # -- End of test case for Bug-- #21380.

-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #

--echo
--echo -- # --
--echo -- # -- Bug-- #18834: ALTER TABLE ADD INDEX on table with two timestamp fields
--echo -- # --
--echo

--disable_warnings
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
--enable_warnings

--echo

CREATE TABLE t1(c1 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, c2 TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00');

--echo

-- # SET sql_mode = 'NO_ZERO_DATE';

--echo
CREATE TABLE t2(c1 TIMESTAMP, c2 TIMESTAMP DEFAULT 0);
DROP TABLE t2;


--echo
--error ER_INVALID_DEFAULT
CREATE TABLE t2(c1 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, c2 TIMESTAMP NOT NULL);

--echo
--echo -- # -- Check that NULL column still can be created.
CREATE TABLE t2(c1 TIMESTAMP NULL);

--echo
--echo -- # -- Check ALTER TABLE.
ALTER TABLE t1 ADD INDEX(c1);

--echo
--echo -- # -- Check DATETIME.
-- # SET sql_mode = '';

--echo

CREATE TABLE t3(c1 DATETIME NOT NULL) ENGINE=MYISAM;
INSERT INTO t3 VALUES (0);

--echo
-- # SET sql_mode = TRADITIONAL;

--echo
--error ER_TRUNCATED_WRONG_VALUE
ALTER TABLE t3 ADD INDEX(c1);

--echo
--echo -- # -- Cleanup.

-- # SET sql_mode = '';
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

--echo
--echo -- # -- End of Bug-- #18834.

-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #-- #

--echo
--echo -- # --
--echo -- # -- Bug-- #34274: Invalid handling of 'DEFAULT 0' for YEAR data type.
--echo -- # --
--echo

--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings

--echo
CREATE TABLE t1(c1 YEAR DEFAULT 2008, c2 YEAR DEFAULT 0);

--echo
SHOW CREATE TABLE t1;

--echo
INSERT INTO t1 VALUES();

--echo
SELECT * FROM t1;

--echo
ALTER TABLE t1 MODIFY c1 YEAR DEFAULT 0;

--echo
SHOW CREATE TABLE t1;

--echo
INSERT INTO t1 VALUES();

--echo
SELECT * FROM t1;

--echo
DROP TABLE t1;

--echo -- # 
--echo -- # Bug -- #22909 'Using CREATE ... LIKE is possible to create field
--echo -- #             with invalid default value'
--echo -- #
--echo -- # Altough original bug report suggests to use older version of MySQL
--echo -- # for producing .FRM with invalid defaults we use sql_mode to achieve
--echo -- # the same effect.
--disable_warnings
drop tables if exists t1, t2;
create table t1 (dt datetime default '2008-02-31 00:00:00');
create table t2 like t1;
--echo -- # Check that table definitions match
show create table t1;
show create table t2;
-- # set @@sql_mode= @old_mode;
drop tables t1, t2;
-- #
-- # Bug-- #47132 CREATE TABLE.. SELECT.. data not inserted if table
-- # is view over multiple tables
-- #

CREATE TABLE t1 (id int);
CREATE TABLE t2 (id int);
INSERT INTO t1 VALUES (1), (1);
INSERT INTO t2 VALUES (2), (2);

CREATE VIEW v1 AS SELECT id FROM t2;
CREATE TABLE IF NOT EXISTS v1(a int, b int) SELECT id, id FROM t1;
SHOW CREATE TABLE v1;
SELECT * FROM t2;
SELECT * FROM v1;
DROP VIEW v1;

CREATE TEMPORARY TABLE tt1 AS SELECT id FROM t2;
CREATE TEMPORARY TABLE IF NOT EXISTS tt1(a int, b int) SELECT id, id FROM t1;
SELECT * FROM t2;
SELECT * FROM tt1;
DROP TEMPORARY TABLE tt1;

DROP TABLE t1, t2;


--echo -- #
--echo -- # WL-- #5370 'Changing 'CREATE TABLE IF NOT EXISTS ... SELECT'
--echo -- # behaviour.
--echo -- # 

--echo -- #
--echo -- # 1. Basic case: a base table.
--echo -- # 

create table if not exists t1 (a int) select 1 as a;
select * from t1;
--error ER_TABLE_EXISTS_ERROR
create table t1 (a int) select 2 as a;
select * from t1;
--echo -- # Produces an essential warning ER_TABLE_EXISTS.
create table if not exists t1 (a int) select 2 as a;
--echo -- # No new data in t1.
select * from t1;
drop table t1;

--echo -- # 
--echo -- # 2. A temporary table.
--echo -- #

create temporary table if not exists t1 (a int) select 1 as a;
select * from t1;
--error ER_TABLE_EXISTS_ERROR
create temporary table t1 (a int) select 2 as a;
select * from t1;
--echo -- # An essential warning.
create temporary table if not exists t1 (a int) select 2 as a;
--echo -- # No new data in t1.
select * from t1;
drop temporary table t1;

--echo -- # 
--echo -- # 3. Creating a base table in presence of a temporary table.
--echo -- #

create table t1 (a int);
--echo -- # Create a view for convenience of querying t1 shadowed by a temp.
create view v1 as select a from t1;
drop table t1;
create temporary table t1 (a int) select 1 as a;
create table if not exists t1 (a int) select 2 as a;
select * from t1;
select * from v1;
--echo -- # Note: an essential warning.
create table if not exists t1 (a int) select 3 as a;
select * from t1;
select * from v1;
drop temporary table t1;
select * from t1;
drop view v1;
drop table t1;

--echo -- # 
--echo -- # 4. Creating a temporary table in presence of a base table.
--echo -- #

create table t1 (a int) select 1 as a;
create temporary table if not exists t1 select 2 as a;
select * from t1;
--echo -- # Note: an essential warning.
create temporary table if not exists t1 select 3 as a;
select * from t1;
drop temporary table t1;
select * from t1;
drop table t1;

--echo -- #
--echo -- # 5. Creating a base table in presence of an updatable view.
--echo -- # 
create table t2 (a int unique);
create view t1 as select a from t2;
insert into t1 (a) values (1);
--error ER_TABLE_EXISTS_ERROR
create table t1 (a int);
--echo -- # Note: an essential warning.
create table if not exists t1 (a int);
--error ER_TABLE_EXISTS_ERROR
create table t1 (a int) select 2 as a;
select * from t1;
--echo -- # Note: an essential warning.
create table if not exists t1 (a int) select 2 as a;
select * from t1;
select * from t2;
create temporary table if not exists t1 (a int) select 3 as a;
select * from t1;
select * from t2;
--echo -- # Note: an essential warning.
create temporary table if not exists t1 (a int) select 4 as a;
select * from t1;
select * from t2;
drop temporary table t1;

--echo -- #
--echo -- # Repeating the test with a non-updatable view.
--echo -- #
drop view t1;
create view t1 as select a + 5 as a from t2;
--error ER_NONUPDATEABLE_COLUMN
insert into t1 (a) values (1);
--error ER_NONUPDATEABLE_COLUMN
update t1 set a=3 where a=2;

--error ER_TABLE_EXISTS_ERROR
create table t1 (a int);
--echo -- # Note: an essential warning.
create table if not exists t1 (a int);
--error ER_TABLE_EXISTS_ERROR
create table t1 (a int) select 2 as a;
select * from t1;
--echo -- # Note: an essential warning.
create table if not exists t1 (a int) select 2 as a;
select * from t1;
select * from t2;
create temporary table if not exists t1 (a int) select 3 as a;
select * from t1;
select * from t2;
--echo -- # Note: an essential warning.
create temporary table if not exists t1 (a int) select 4 as a;
select * from t1;
select * from t2;
drop temporary table t1;
drop view t1;
drop table t2;

--echo -- #
--echo -- # Repeating the test with a view select a constant number
--echo -- #
create view t1 as select 1 as a;
--error ER_NON_INSERTABLE_TABLE
insert into t1 (a) values (1);
--error ER_NON_UPDATABLE_TABLE
update t1 set a=3 where a=2;

--error ER_TABLE_EXISTS_ERROR
create table t1 (a int);
--echo -- # Note: an essential warning.
create table if not exists t1 (a int);
--error ER_TABLE_EXISTS_ERROR
create table t1 (a int) select 2 as a;
select * from t1;
--echo -- # Note: an essential warning.
create table if not exists t1 (a int) select 2 as a;
select * from t1;
create temporary table if not exists t1 (a int) select 3 as a;
select * from t1;
--echo -- # Note: an essential warning.
create temporary table if not exists t1 (a int) select 4 as a;
select * from t1;
drop temporary table t1;
drop view t1;


--echo -- #
--echo -- # 6. Test of unique_table().
--echo -- #

create table t1 (a int) select 1 as a;
create temporary table if not exists t1 (a int) select * from t1;
--error ER_CANT_REOPEN_TABLE
create temporary table if not exists t1 (a int) select * from t1;
select * from t1;
drop temporary table t1;
select * from t1;
drop table t1;
create temporary table t1 (a int) select 1 as a;
create table if not exists t1 (a int) select * from t1;
create table if not exists t1 (a int) select * from t1;
select * from t1;
drop temporary table t1;
select * from t1;
drop table t1;
--error ER_NO_SUCH_TABLE
create table if not exists t1 (a int) select * from t1;

--echo -- #
--echo -- # 7. Test of non-matching columns, REPLACE and IGNORE.
--echo -- #

create table t1 (a int) select 1 as b, 2 as c;
select * from t1;
drop table t1;
create table if not exists t1 (a int, b date, c date) select 1 as b, 2 as c;
select * from t1;
drop table t1;
-- # set @@session.sql_mode=default;
--error ER_TRUNCATED_WRONG_VALUE
create table if not exists t1 (a int, b date, c date) select 1 as b, 2 as c;
--error ER_NO_SUCH_TABLE
select * from t1;
--error ER_TRUNCATED_WRONG_VALUE
create table if not exists t1 (a int, b date, c date) 
  replace select 1 as b, 2 as c;
--error ER_NO_SUCH_TABLE
select * from t1;

create table if not exists t1 (a int, b date, c date) 
  ignore select 1 as b, 2 as c;
select * from t1;
drop table t1;

create table if not exists t1 (a int unique, b int)
  replace select 1 as a, 1 as b union select 1 as a, 2 as b;
select * from t1;
drop table t1;
create table if not exists t1 (a int unique, b int)
  ignore select 1 as a, 1 as b union select 1 as a, 2 as b;
select * from t1;
drop table t1;
--echo -- #

--echo -- #
--echo -- # WL-- #5576 Prohibit CREATE TABLE ... SELECT to modify other tables
--echo -- #

delimiter |;
create function f()
returns int
begin
insert into t2 values(1);
return 1;
end|
delimiter ;|

--echo -- #
--echo -- # 1. The function updates a base table
--echo -- #
create table t2(c1 int);

--error ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT
create table t1 select f();
--error ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT
create temporary table t1 select f();


drop table t2;

--echo -- #
--echo -- # 2. The function updates a view which derives from a base table
--echo -- #
create table t3(c1 int);
create view t2 as select c1 from t3;

--error ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT
create table t1 select f();
--error ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT
create temporary table t1 select f();

drop view t2;

--echo -- #
--echo -- # 3. The function updates a view which derives from two base tables
--echo -- #
create table t4(c1 int);
create view t2 as select t3.c1 as c1 from t3, t4;

--error ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT
create table t1 select f();
--error ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT
create temporary table t1 select f();

drop view t2;
drop tables t3, t4;

--echo -- #
--echo -- # 4. The function updates a view which selects a constant number
--echo -- #
create view t2 as select 1;

--error ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT
create table t1 select f();
--error ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT
create temporary table t1 select f();

drop view t2;
drop function f;

--echo -- #
--echo -- # BUG-- #11762377 - 54963: ENHANCE THE ERROR MESSAGE TO 
--echo -- #                       REDUCE USER CONFUSION 
--echo -- #

--error ER_TOO_BIG_ROWSIZE
CREATE TABLE t1 (v varchar(65535));

--echo -- #
--echo -- # Bug-- #11746295 - 25168: 'INCORRECT TABLE NAME' INSTEAD OF 'IDENTIFIER TOO
--echo -- #                       LONG' IF TABLE NAME > 64 CHARACTERS
--echo -- #

--error ER_TOO_LONG_IDENT
CREATE TABLE t01234567890123456789012345678901234567890123456789012345678901234567890123456789(a int);
--error ER_TOO_LONG_IDENT
CREATE DATABASE t01234567890123456789012345678901234567890123456789012345678901234567890123456789;

--echo -- #
--echo -- # Bug -- #20573701 DROP DATABASE ASSERT ON DEBUG WHEN OTHER FILES PRESENT IN
--echo -- #               DB FOLDER.
--echo -- #
-- let $MYSQLD_DATADIR= `SELECT @@datadir`;
-- # Case 1: A database with no tables and has an unrelated file in it's database
-- #         directory. Dropping such database should throw ER_DB_DROP_RMDIR
-- #         error.
CREATE DATABASE db_with_no_tables_and_an_unrelated_file_in_data_directory;
--write_file $MYSQLD_DATADIR/db_with_no_tables_and_an_unrelated_file_in_data_directory/intruder.txt
-- EOF
--replace_result $MYSQLD_DATADIR ./ \\ /
--error ER_DB_DROP_RMDIR
DROP DATABASE db_with_no_tables_and_an_unrelated_file_in_data_directory;
-- # Cleanup
--remove_file $MYSQLD_DATADIR/db_with_no_tables_and_an_unrelated_file_in_data_directory/intruder.txt
DROP DATABASE db_with_no_tables_and_an_unrelated_file_in_data_directory;

-- # Case 2: A database with tables in it and has an unrelated file in it's database
-- #         directory. Dropping such database should throw ER_DB_DROP_RMDIR
-- #         error.
CREATE DATABASE db_with_tables_and_an_unrelated_file_in_data_directory;
--write_file $MYSQLD_DATADIR/db_with_tables_and_an_unrelated_file_in_data_directory/intruder.txt
-- EOF
--replace_result $MYSQLD_DATADIR ./ \\ /
--error ER_DB_DROP_RMDIR
DROP DATABASE db_with_tables_and_an_unrelated_file_in_data_directory;
-- # Cleanup
--remove_file $MYSQLD_DATADIR/db_with_tables_and_an_unrelated_file_in_data_directory/intruder.txt
DROP DATABASE db_with_tables_and_an_unrelated_file_in_data_directory;

-- # Case 3: A database (fakely created using mkdir) and has an unrelated file in it's database
-- #         directory. Dropping such database should throw ER_DB_DROP_RMDIR
-- #         error.
--mkdir $MYSQLD_DATADIR/db_created_with_mkdir_and_an_unrelated_file_in_data_directory
--write_file $MYSQLD_DATADIR/db_created_with_mkdir_and_an_unrelated_file_in_data_directory/intruder.txt
-- EOF
--replace_result $MYSQLD_DATADIR ./ \\ /
--error ER_DB_DROP_RMDIR
DROP DATABASE db_created_with_mkdir_and_an_unrelated_file_in_data_directory;
-- # Cleanup
--remove_file $MYSQLD_DATADIR/db_created_with_mkdir_and_an_unrelated_file_in_data_directory/intruder.txt
DROP DATABASE db_created_with_mkdir_and_an_unrelated_file_in_data_directory;


--echo -- #
--echo -- # BUG 27516741 - MYSQL SERVER DOES NOT WRITE INNODB ROW_TYPE
--echo -- #                TO .FRM FILE WHEN DEFAULT USED

--echo -- # Set up.
-- # SET @saved_innodb_default_row_format= @@global.innodb_default_row_format;
-- # SET @saved_show_create_table_verbosity= @@session.show_create_table_verbosity;

--echo -- # Current InnoDB default row format and 'show_create_table_verbosity'
--echo -- # values respectively.
-- SELECT @@global.innodb_default_row_format;
-- SELECT @@session.show_create_table_verbosity;

CREATE TABLE t1(fld1 INT) ENGINE= InnoDB;
CREATE TABLE t2(fld1 INT) ENGINE= InnoDB, ROW_FORMAT= DEFAULT;
-- # SET GLOBAL innodb_default_row_format= 'COMPACT';
CREATE TABLE t3(fld1 INT) ENGINE= InnoDB;
CREATE TABLE t4(fl1 INT) ENGINE= InnoDB, ROW_FORMAT= COMPRESSED;

--echo -- # Test without show_create_table_verbosity enabled.
--echo -- # Row format used is not displayed for all tables
--echo -- # except t4 where it is explicitly specified.
SHOW CREATE TABLE t1;
SHOW CREATE TABLE t2;
SHOW CREATE TABLE t3;
SHOW CREATE TABLE t4;

--echo -- # Test with show_create_table_verbosity enabled.
--echo -- # Row format used is displayed for all tables.
-- # SET SESSION show_create_table_verbosity= ON;
SHOW CREATE TABLE t1;
SHOW CREATE TABLE t2;
SHOW CREATE TABLE t3;
SHOW CREATE TABLE t4;

-- # SET GLOBAL innodb_default_row_format= 'DYNAMIC';
-- # SET SESSION show_create_table_verbosity= OFF;

--echo -- # Test with corresponding temporary tables.
CREATE TEMPORARY TABLE t1(fld1 INT) ENGINE= InnoDB;
CREATE TEMPORARY TABLE t2(fld1 INT) ENGINE= InnoDB, ROW_FORMAT= DEFAULT;
-- # SET GLOBAL innodb_default_row_format= 'COMPACT';
CREATE TEMPORARY TABLE t3(fld1 INT) ENGINE= InnoDB;
CREATE TEMPORARY TABLE t4(fl1 INT) ENGINE= InnoDB, ROW_FORMAT= COMPRESSED;

--echo -- # Test without show_create_table_verbosity enabled.
--echo -- # Row format used is not displayed for all tables
--echo -- # except t4 where it is explicitly specified.
SHOW CREATE TABLE t1;
SHOW CREATE TABLE t2;
SHOW CREATE TABLE t3;
SHOW CREATE TABLE t4;

--echo -- # Test with show_create_table_verbosity enabled.
--echo -- # Row format used is displayed for all tables.
-- # SET SESSION show_create_table_verbosity= ON;
SHOW CREATE TABLE t1;
SHOW CREATE TABLE t2;
SHOW CREATE TABLE t3;
SHOW CREATE TABLE t4;

--echo -- # Clean up.
DROP TABLE t1, t2, t3, t4;
DROP TABLE t1, t2, t3, t4;
-- # SET GLOBAL innodb_default_row_format= @saved_innodb_default_row_format;
-- # SET SESSION show_create_table_verbosity= @saved_show_create_table_verbosity;


--echo -- #
--echo -- # Bug-- #28022129: NOW() DOESN?T HONOR NO_ZERO_DATE SQL_MODE
--echo -- #

-- # SET @saved_mode= @@sql_mode;

CREATE TABLE t1(fld1 int);

--echo -- # NO_ZERO_DATE and STRICT SQL mode.

CREATE TABLE t2 SELECT fld1, CURDATE() fld2 FROM t1;
CREATE TABLE t3 AS SELECT now();

--echo -- # With patch, zero date is not generated as default.
SHOW CREATE TABLE t2;
SHOW CREATE TABLE t3;
DROP TABLE t2, t3;

-- # SET SQL_MODE= 'NO_ZERO_DATE';

--echo -- # NO_ZERO_DATE SQL mode.
CREATE TABLE t2 SELECT fld1, CURDATE() fld2 FROM t1;
CREATE TABLE t3 AS SELECT now();

--echo -- # Zero date is generated as default in non strict mode.
SHOW CREATE TABLE t2;
SHOW CREATE TABLE t3;
DROP TABLE t1, t2, t3;

-- # SET SQL_MODE= DEFAULT;

--echo -- # Test cases added for coverage.

CREATE TABLE t1(fld1 DATETIME NOT NULL DEFAULT '1111:11:11');

--echo -- # CREATE TABLE..SELECT using fields of another table.
CREATE TABLE t2 AS SELECT * FROM t1;
--echo -- # Default value is copied from the source table column.
SHOW CREATE TABLE t2;

DROP TABLE t1, t2;

--echo -- # CREATE TABLE..SELECT based on trigger fields.
CREATE TABLE t1 (fld1 INT, fld2 DATETIME DEFAULT '1211:1:1');

DELIMITER |;
CREATE TRIGGER t1_bi BEFORE INSERT ON t1
FOR EACH ROW
BEGIN
  CREATE TEMPORARY TABLE t2 AS SELECT NEW.fld1, NEW.fld2;
END
|
DELIMITER ;|

INSERT INTO t1 VALUES (1, '1111:11:11');
SHOW CREATE TABLE t2;

DROP TABLE t1;
DROP TEMPORARY TABLE t2;
-- # SET SQL_MODE= @saved_mode;
