-- Test needs myisam for subtest for Bug#32705
-- source include/have_myisam.inc
-- source include/have_ucs2.inc

--
-- MySQL Bug#15276: MySQL ignores collation-server
--
show variables like 'collation_server';

--
-- Bug#18004 Connecting crashes server when default charset is UCS2
--
show variables like "%character_set_ser%";
--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings
create table t1 (a int);
drop table t1;

--echo End of 4.1 tests

--
-- Bug #28925 GROUP_CONCAT inserts wrong separators for a ucs2 column
-- Check that GROUP_CONCAT works fine with --default-character-set=ucs2
--
create table t1 (a char(1) character set latin1);
insert into t1 values ('a'),('b'),('c');
select hex(group_concat(a)) from t1;
drop table t1;

--echo End of 5.0 tests
