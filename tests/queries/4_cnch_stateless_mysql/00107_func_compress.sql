-- # The include statement below is a temp one for tests that are yet to
-- #be ported to run with InnoDB,
-- #but needs to be kept for tests that would need MyISAM in future.
--source include/force_myisam_default.inc

-- source include/have_compress.inc
-- #
-- # Test for compress and uncompress functions:
-- #
-- # Note that this test gives error in the gzip library when running under
-- # valgrind, but these warnings can be ignored

select 'string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ';
select length('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ');

select uncompress(compress('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa '));
explain extended select uncompress(compress('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa '));
select uncompressed_length(compress('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa '))=length('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ');
explain extended select uncompressed_length(compress('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa '))=length('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ');
select uncompressed_length(compress('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa '));
select length(compress('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa '))<length('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ');

create table t1 (a text, b char(255), c char(4)) engine=myisam;
insert into t1 (a,b,c) values (compress('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa '),compress('string for test compress function aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa '),'d ');
select uncompress(a) from t1;
select uncompress(b) from t1;
select concat('|',c,'|') from t1;
drop table t1;

select compress("");
select uncompress("");
select uncompress(compress(""));
select uncompressed_length("");

-- #
-- # errors
-- #

create table t1 (a text);
insert t1 values (compress(null)), ('A\0\0\0BBBBBBBB'), (compress(space(50000))), (space(50000));
select length(a) from t1;
select length(uncompress(a)) from t1;
drop table t1;


-- #
-- # Bug #18643: problem with null values
-- #

create table t1(a blob);
insert into t1 values(NULL), (compress('a'));
select uncompress(a), uncompressed_length(a) from t1;
drop table t1;

-- #
-- # Bug #23254: problem with compress(NULL)
-- #

create table t1(a blob);
insert into t1 values ('0'), (NULL), ('0');
--disable_result_log
select compress(a), compress(a) from t1;
--enable_result_log
select compress(a) is null from t1;
drop table t1;

--echo End of 4.1 tests

-- #
-- # Bug #18539: uncompress(d) is null: impossible?
-- #
create table t1 (a varchar(32) not null);
insert into t1 values ('foo');
explain select * from t1 where uncompress(a) is null;
select * from t1 where uncompress(a) is null;
explain select *, uncompress(a) from t1;
select *, uncompress(a) from t1;
select *, uncompress(a), uncompress(a) is null from t1;
drop table t1;

-- #
-- # Bug #44796: valgrind: too many my_longlong10_to_str_8bit warnings after 
-- #             uncompressed_length
-- #

CREATE TABLE t1 (c1 INT);
INSERT INTO t1 VALUES (1), (1111), (11111);

-- # Disable warnings to avoid dependency on max_allowed_packet value
--disable_warnings
SELECT UNCOMPRESS(c1), UNCOMPRESSED_LENGTH(c1) FROM t1;
--enable_warnings

-- # We do not need the results, just make sure there are no valgrind errors
--disable_result_log
EXPLAIN EXTENDED SELECT * FROM (SELECT UNCOMPRESSED_LENGTH(c1) FROM t1) AS s;
--enable_result_log

DROP TABLE t1;

--echo End of 5.0 tests

--echo #
--echo # Bug#18693654 VALGRIND WARNINGS IN INFLATE ON UNCOMPRESS
--echo #

SELECT UNCOMPRESS( CAST( 0 AS BINARY(5) ) );
