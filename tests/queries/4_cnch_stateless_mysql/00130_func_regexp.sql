-- The include statement below is a temp one for tests that are yet to
--be ported to run with InnoDB,
--but needs to be kept for tests that would need MyISAM in future.
--source include/force_myisam_default.inc

--
-- Some regexp tests
--

--disable_warnings
drop table if exists t1;
--enable_warnings

-- set names latin1;
--source include/ctype_regex.inc


--
-- This test a bug in regexp on Alpha
--

create table t1 (xxx char(128));
insert into t1 (xxx) values('this is a test of some long text to see what happens');
select * from t1 where xxx regexp('is a test of some long text to');
explain select * from t1 where xxx regexp('is a test of some long text to');
select * from t1 where xxx regexp('is a test of some long text to ');
select * from t1 where xxx regexp('is a test of some long text to s');
select * from t1 where xxx regexp('is a test of some long text to se');
drop table t1;

create table t1 (xxx char(128));
insert into t1 (xxx) values('this is some text: to test - out.reg exp (22/45)');
select * from t1 where xxx REGEXP '^this is some text: to test - out\\.reg exp [[(][0-9]+[/\\][0-9]+[])][ ]*$';
drop table t1;

--echo End of 4.1 tests


--
-- Bug #31440: 'select 1 regex null' asserts debug server
--

SELECT 1 REGEXP NULL;


--
-- Bug #39021: SELECT REGEXP BINARY NULL never returns
--

SELECT '' REGEXP BINARY NULL;
SELECT NULL REGEXP BINARY NULL;
SELECT 'A' REGEXP BINARY NULL;
SELECT 'ABC' REGEXP BINARY NULL;

--echo End of 5.0 tests

-- Bug #54805 definitions in regex/my_regex.h conflict with /usr/include/regex.h
--
-- This test verifies that '\t' is recognized as space, but not as blank.
-- This is *not* according to the POSIX standard, but seems to have been MySQL
-- behaviour ever since regular expressions were introduced.
-- See: Bug #55427 REGEXP does not recognize '\t' as [:blank:]
--
SELECT ' '  REGEXP '[[:blank:]]';
SELECT '\t' REGEXP '[[:blank:]]';

SELECT ' '  REGEXP '[[:space:]]';
SELECT '\t' REGEXP '[[:space:]]';
