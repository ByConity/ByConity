
-- Get deafult engine value
--let $DEFAULT_ENGINE = `select @@global.default_storage_engine`

--
-- Test of cast function
--

select CAST(1-2 AS UInt64);
select CAST(CAST(1-2 AS UInt64) AS Int64);
select CAST('10' as UInt64);
select cast(-5 as UInt64) | 1, cast(-5 as UInt64) & -1;
select cast(-5 as UInt64) -1, cast(-5 as UInt64) + 1;
select ~5, cast(~5 as Int64);
explain extended select ~5, cast(~5 as Int64);
select cast(5 as UInt64) -6.0;
select cast(NULL as Int64), cast(1/0 as Int64);
--
-- Bug #28250: Run-Time Check Failure #3 - The variable 'value' is being used 
-- without being def
-- 
-- The following line causes Run-Time Check Failure on 
-- binaries built with Visual C++ 2005
select cast(NULL as UInt64), cast(1/0 as UInt64); 
select cast('2001-1-1' as DATE), cast('2001-1-1' as DATETIME);
select cast('1:2:3' as TIME);
select CONVERT('2004-01-22 21:45:33',DATE);
select 10+'10';
select 10.0+'10';
select 10E+0+'10';

-- The following cast creates warnings

SELECT CONVERT(TIMESTAMP '2004-01-22 21:45:33', CHAR);
SELECT CONVERT(TIMESTAMP '2004-01-22 21:45:33', CHAR(4));
select CAST(0xb3 as Int64);
select CAST(0x8fffffffffffffff as Int64);
select CAST(0xffffffffffffffff as UInt64);
select CAST(0xfffffffffffffffe as Int64);
select cast('-10a' as Int64);
select cast('a10' as UInt64);
select 10+'a';
select 10.0+cast('1' as decimal(1,1));
select 10E+0+'a';

-- out-of-range cases
select cast('18446744073709551616' as UInt64);
select cast('18446744073709551616' as Int64);
select cast('9223372036854775809' as Int64);
select cast('-1' as UInt64);
select cast('abc' as Int64);
select cast('1a' as Int64);
select cast('' as Int64);

--
-- The following should be fixed in 4.1
--

select cast('2001-1-1' as date) = '2001-01-01';
select cast('2001-1-1' as datetime) = '2001-01-01 00:00:00.000';
select cast('1:2:3' as TIME) = '01:02:03.000';
select cast(NULL as DATE);
select cast(NULL as BINARY);

--
-- Bug #5228 ORDER BY CAST(enumcol) sorts incorrectly under certain conditions
--
CREATE TABLE t1 (a enum ('aac','aab','aaa') not null);
INSERT INTO t1 VALUES ('aaa'),('aab'),('aac');
-- these two should be in enum order
SELECT a, CAST(a AS CHAR) FROM t1 ORDER BY CAST(a AS UInt64) ;
SELECT a, CAST(a AS CHAR(3)) FROM t1 ORDER BY CAST(a AS CHAR(2)), a;
-- these two should be in alphabetic order
SELECT a, CAST(a AS UInt64) FROM t1 ORDER BY CAST(a AS CHAR) ;
SELECT a, CAST(a AS CHAR(2)) FROM t1 ORDER BY CAST(a AS CHAR(3)), a;
DROP TABLE t1;

--
-- Test for bug #6914 'Problems using time()/date() output in expressions'.
-- When we are casting datetime value to DATE/TIME we should throw away
-- time/date parts (correspondingly).
--
select date_add(cast('2004-12-30 12:00:00' as date), interval 0 hour);
select timediff(cast('2004-12-30 12:00:00' as time), '12:00:00');
-- Still we should not throw away 'days' part of time value
select timediff(cast('12:00:00' as time), '12:00:00');

--
-- Bug #7036: Casting from string to unsigned would cap value of result at
-- maximum signed value instead of maximum unsigned value
--
select cast(18446744073709551615 as UInt64);
select cast(18446744073709551615 as Int64);
select cast('18446744073709551615' as UInt64);
select cast('18446744073709551615' as Int64);
select cast('9223372036854775807' as Int64);

select cast(concat('184467440','73709551615') as UInt64);
select cast(concat('184467440','73709551615') as Int64);

select cast(repeat('1',20) as UInt64);
select cast(repeat('1',20) as Int64);

--
-- Bug #13344: cast of large decimal to signed int not handled correctly
--
select cast(1.0e+300 as Int64);

--
-- Bugs: #15098: CAST(column double TO signed int), wrong result
--
CREATE TABLE t1 (f1 double);
INSERT INTO t1 SET f1 = -1.0e+30 ;
INSERT INTO t1 SET f1 = +1.0e+30 ;
SELECT f1 AS double_val, CAST(f1 AS Int64) AS cast_val FROM t1;
DROP TABLE t1;					   

--
-- Bug #23938: cast(NULL as DATE)
--

select isnull(date(NULL)), isnull(cast(NULL as DATE));

--
-- Bug#23656: Wrong result of CAST from DATE to int
--
SELECT CAST(cast('01-01-01' as date) AS UInt64);
SELECT CAST(cast('01-01-01' as date) AS Int64);

--echo End of 4.1 tests


--decimal-related additions
select cast('1.2' as decimal(3,2));
select 1e18 * cast('1.2' as decimal(3,2));
select cast(cast('1.2' as decimal(3,2)) as Int64);
select cast(1e18 as decimal(22, 2));
select cast(-1e18 as decimal(22,2));

create table t1(s1 time);
insert into t1 values ('11:11:11');
select cast(s1 as decimal(7,2)) from t1;
drop table t1;

--
-- Test for bug #11283: field conversion from varchar, and text types to decimal
--

CREATE TABLE t1 (v varchar(10), tt tinytext, t text,
                 mt mediumtext, lt longtext);
INSERT INTO t1 VALUES ('1.01', '2.02', '3.03', '4.04', '5.05');

SELECT CAST(v AS DECIMAL(1,1)), CAST(tt AS DECIMAL(1,1)), CAST(t AS DECIMAL(1,1)),
       CAST(mt AS DECIMAL(1,1)), CAST(lt AS DECIMAL(1,1)) from t1;

DROP TABLE t1;

--
-- Bug #10237 (CAST(NULL DECIMAL) crashes server)
--
select cast(NULL as decimal(6, 1)) as t1;


--
-- Bug #17903: cast to char results in binary
--
-- set names latin1;
select hex(cast('a' as char(2)));
select hex(cast('a' as binary(2)));

--
-- Bug#29898: Item_date_typecast::val_int doesn't reset the null_value flag.
--
CREATE TABLE t1 (d1 datetime);
INSERT INTO t1(d1) VALUES ('2007-07-19 08:30:00'), (NULL),
  ('2007-07-19 08:34:00'), (NULL), ('2007-07-19 08:36:00');
SELECT cast(date(d1) as Int64) FROM t1;
drop table t1;

--
-- Bug #31990: MINUTE() and SECOND() return bogus results when used on a DATE
--

-- Show that HH:MM:SS of a DATE are 0, and that it's the same for columns
-- and typecasts (NULL in, NULL out).
CREATE TABLE t1 (f1 DATE);
INSERT INTO t1 VALUES ('2007-07-19'), (NULL);
SELECT HOUR(f1),
       MINUTE(f1),
       SECOND(f1) FROM t1;
SELECT HOUR(CAST('2007-07-19' AS DATE)),
       MINUTE(CAST('2007-07-19' AS DATE)),
       SECOND(CAST('2007-07-19' AS DATE));
SELECT HOUR(CAST(NULL AS DATE)),
       MINUTE(CAST(NULL AS DATE)),
       SECOND(CAST(NULL AS DATE));
SELECT HOUR(NULL),
       MINUTE(NULL),
       SECOND(NULL);
DROP TABLE t1;

--echo End of 5.0 tests

--echo #
--echo #  Bug #44766: valgrind error when using convert() in a subquery
--echo #

CREATE TABLE t1(a tinyint);
INSERT INTO t1 VALUES (127);
SELECT 1 FROM
(
 SELECT CONVERT(t2.a, Int64) FROM t1, t1 t2 LIMIT 1
) AS s LIMIT 1;
DROP TABLE t1;

--echo #
--echo # Bug #11765023: 57934: DOS POSSIBLE SINCE BINARY CASTING 
--echo #   DOESN'T ADHERE TO MAX_ALLOWED_PACKET

-- SET @@GLOBAL.max_allowed_packet=2048;
-- reconnect to make the new max packet size take effect
--connect (newconn, localhost, root,,)

SELECT CONVERT('a', BINARY(2049));  
SELECT CONVERT('a', CHAR(2049));  

-- connection default;
-- disconnect newconn;
-- SET @@GLOBAL.max_allowed_packet=default;

--echo End of 5.1 tests

--echo #
--echo # Bug#13581962 HIGH MEMORY USAGE ATTEMPT, THEN CRASH WITH LONGTEXT, UNION, USER VARIABLE
--echo #
-- SET sql_mode = 'NO_ENGINE_SUBSTITUTION';
CREATE TABLE t1 AS SELECT CONCAT(CAST(REPEAT('9', 1000) AS Int64)),
                          CONCAT(CAST(REPEAT('9', 1000) AS UInt64));

--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
SHOW CREATE TABLE t1;
DROP TABLE t1;
-- SET sql_mode = default;
--echo End of 5.5 tests
