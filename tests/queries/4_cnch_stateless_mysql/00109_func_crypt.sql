
-- source include/have_crypt.inc

--disable_warnings
drop table if exists t1;
--enable_warnings

select length(encrypt('foo', 'ff')) <> 0;
--replace_result $1$aa$4OSUA5cjdx0RUQ08opV27/ aaqPiZY5xR5l.

create table t1 (name varchar(50), pw varchar(64));
--disable_warnings
insert into t1 values ('tom', password('my_pass'));
-- set @pass='my_pass';
select name from t1 where name='tom' and pw=password(@pass);
select name from t1 where name='tom' and pw=password(@undefined);
drop table t1;

-- Test new and old password handling functions 

select password('abc');
select password('');
select password('gabbagabbahey');
select length(password('1'));
select length(encrypt('test'));
select encrypt('test','aa');
select password(NULL);

-- this test shows that new scrambles honor spaces in passwords:
-- set old_passwords=0;
select password('idkfa ');
select password('idkfa');
select password(' idkfa');

explain extended select password('idkfa ');
--enable_warnings

--
-- Bug #13619: Crash on FreeBSD with salt like '_.'
--
--replace_column 1 #
select encrypt('1234','_.');

-- End of 4.1 tests

--echo #
--echo # Bug #44767: invalid memory reads in password()
--echo #             functions
--echo #

CREATE TABLE t1(c1 MEDIUMBLOB);
INSERT INTO t1 VALUES (REPEAT('a', 1024));
--disable_warnings
SELECT PASSWORD(c1) FROM t1;
--enable_warnings
DROP TABLE t1;

--echo End of 5.0 tests

--echo #
--echo # Start of 5.6 tests
--echo #

--echo #
--echo # Bug#13812875 ILLEGAL MIX OF COLLATIONS WITH FUNCTIONS THAT USED TO WORK
--echo #
-- SET NAMES utf8;
CREATE TABLE t1 (a varchar(1));
--disable_warnings
SELECT * FROM t1 WHERE a=password('a');
--enable_warnings
DROP TABLE t1;
-- SET NAMES latin1;

--echo #
--echo # End of 5.6 tests
--echo #
