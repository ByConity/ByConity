drop table if exists t40099_x;
drop table if exists t40099_y;

create table t40099_x(a Int32, b Int32, c Int32, d Int32) engine=CnchMergeTree() order by tuple();
insert into t40099_x values (1, 10, 100, 1000);

create table t40099_y(a Int32, b Int32) engine=CnchMergeTree() order by tuple();
insert into t40099_y values (1, 20);

SET dialect_type = 'MYSQL';
SET allow_mysql_having_name_resolution = 1;

-- { echoOn }

SELECT a FROM t40099_x HAVING b = 10;

-- "b" refer to "(1 + 1) as b"
SELECT a, (1 + 1) as b FROM t40099_x HAVING b = 10;
EXPLAIN SELECT a, (1 + 1) as b FROM t40099_x HAVING b = 10;

-- "b" refer to "(1 > 0) as b"
SELECT a, (1 > 0) as b FROM t40099_x HAVING b;
EXPLAIN SELECT a, (1 > 0) as b FROM t40099_x HAVING b;

-- "b" refer to "sum(c) as b"
SELECT sum(c) as b FROM t40099_x HAVING b = 10;
EXPLAIN SELECT sum(c) as b FROM t40099_x HAVING b = 10;

-- "b" refer to "b"
SELECT b, sum(c) as b FROM t40099_x GROUP BY b HAVING b = 10;
EXPLAIN SELECT b, sum(c) as b FROM t40099_x GROUP BY b HAVING b = 10;

-- "b" refer to "t40099_x.b"
SELECT t40099_x.b, sum(c) as b FROM t40099_x JOIN t40099_y ON t40099_x.a = t40099_y.a GROUP BY t40099_x.b HAVING b = 10;
EXPLAIN SELECT t40099_x.b, sum(c) as b FROM t40099_x JOIN t40099_y ON t40099_x.a = t40099_y.a GROUP BY t40099_x.b HAVING b = 10;

-- "b" refer to "t40099_y.b"
SELECT t40099_y.b, sum(c) as b FROM t40099_x JOIN t40099_y ON t40099_x.a = t40099_y.a GROUP BY t40099_y.b HAVING b = 10;
EXPLAIN SELECT t40099_y.b, sum(c) as b FROM t40099_x JOIN t40099_y ON t40099_x.a = t40099_y.a GROUP BY t40099_y.b HAVING b = 10;

-- "b" is ambiguous as there are 2 items with name "b" in GROUP BY
SELECT t40099_x.b, t40099_y.b, sum(c) as b FROM t40099_x JOIN t40099_y ON t40099_x.a = t40099_y.a GROUP BY t40099_x.b, t40099_y.b HAVING b = 10; -- { serverError 179 }

-- prefer source column under aggregation
SELECT sum(b) as b FROM t40099_x HAVING b = 10;
SELECT sum(b) as b FROM t40099_x HAVING sum(b) = 10;
SELECT sum(b) as b FROM t40099_x HAVING floor(b) = 10;

-- { echoOff }

drop table if exists t40099_x;
drop table if exists t40099_y;
