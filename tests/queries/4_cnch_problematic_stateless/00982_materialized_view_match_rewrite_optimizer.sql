set enable_optimizer=1;
USE test;

DROP TABLE IF EXISTS test.mv_define;
DROP TABLE IF EXISTS test.emps;
DROP TABLE IF EXISTS test.depts;
DROP TABLE IF EXISTS test.locations;
DROP TABLE IF EXISTS test.mv_define;
DROP TABLE IF EXISTS test.mv_data;

CREATE TABLE emps(
    empid UInt32,
    deptno UInt32,
    name Nullable(String),
    salary Nullable(Float64),
    commission Nullable(UInt32)
) ENGINE = CnchMergeTree()
ORDER BY empid;

INSERT INTO emps VALUES (100, 10, 'Bill', 10000, 1000);
INSERT INTO emps VALUES (200, 20, 'Eric', 8000, 500);
INSERT INTO emps VALUES (150, 10, 'Sebastian', 7000, null);
INSERT INTO emps VALUES (110, 10, 'Theodore', 11500, 250);

CREATE TABLE depts(
      deptno UInt32,
      name Nullable(String)
) ENGINE = CnchMergeTree()
ORDER BY deptno;

INSERT INTO depts VALUES (10, 'Sales');
INSERT INTO depts VALUES (30, 'Marketing');
INSERT INTO depts VALUES (40, 'HR');

CREATE TABLE locations(
    locationid UInt32,
    name Nullable(String)
) ENGINE = CnchMergeTree()
ORDER BY locationid;

INSERT INTO locations VALUES (10, 'San Francisco');
INSERT INTO locations VALUES (20, 'San Diego');

-- 1.test projection rewrite
DROP TABLE IF EXISTS test.mv_define;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data ENGINE = CnchMergeTree() ORDER BY deptno AS
select deptno, sum(salary), sum(commission), sum(k)
from (select deptno, salary, commission, 100 as k
      from emps)
group by deptno;

CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select deptno, sum(salary), sum(commission), sum(k)
from (select deptno, salary, commission, 100 as k
      from emps)
group by deptno;

select '1.test projection rewrite';
explain select deptno + 1, sum(salary) / 100, sum(k)
from (select deptno, salary, 100 as k
      from emps)
group by deptno;

select deptno + 1, sum(salary) / 100, sum(k)
from (select deptno, salary, 100 as k
      from emps)
group by deptno
order by deptno;

-- 2. test filter rewrite
DROP TABLE IF EXISTS test.mv_define;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data ENGINE = CnchMergeTree() ORDER BY deptno AS
select deptno, empid, name from emps
where deptno = 10 or deptno = 20 or empid < 160;

CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select deptno, empid, name from emps
where deptno = 10 or deptno = 20 or empid < 160;

select '2-1. test filter rewrite';
explain select empid as x, name from emps where deptno = 10;
select empid as x, name from emps where deptno = 10 order by empid;
select '2-2. test filter rewrite';
explain select empid as x, name from emps where (deptno = 10 or deptno = 20 or empid < 160) and empid > 120;
select empid as x, name from emps where (deptno = 10 or deptno = 20 or empid < 160) and empid > 120 order by empid;

-- 3. test rollup aggregate rewrite
DROP TABLE IF EXISTS test.mv_define;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data ENGINE = CnchMergeTree() ORDER BY deptno AS
select empid, deptno, count(*) as c, sum(empid) as s
from emps group by empid, deptno;

DROP TABLE IF EXISTS test.mv_define;
CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select empid, deptno, count(*) as c, sum(empid) as s
from emps group by empid, deptno;

select '3. test rollup aggregate rewrite';
explain select count(*) + 1 as c, deptno from emps group by deptno;
select count(*) + 1 as c, deptno from emps group by deptno order by deptno;

-- 4. test rollup aggregate rewrite with agg state
DROP TABLE IF EXISTS test.mv_define;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data ENGINE = CnchMergeTree() ORDER BY deptno AS
select empid, deptno, countState(*) as c, sumState(empid) as s
from emps group by empid, deptno;

DROP TABLE IF EXISTS test.mv_define;
CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select empid, deptno, countState(*) as c, sumState(empid) as s
from emps group by empid, deptno;

select '4. test rollup aggregate rewrite';
explain select count(*) as c, deptno from emps group by deptno;
select count(*) as c, deptno from emps group by deptno order by deptno;

-- 5. test rollup aggregate rewrite with filter
DROP TABLE IF EXISTS test.mv_define;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data ENGINE = CnchMergeTree() ORDER BY deptno AS
select empid, deptno, count(*) as c, sum(empid) as s, avg(salary) as a
from emps
group by empid, deptno;

DROP TABLE IF EXISTS test.mv_define;
CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select empid, deptno, count(*) as c, sum(empid) as s, avg(salary) as a
from emps
group by empid, deptno;


select '5-2. test rollup aggregate rewrite with filter';
explain select count(*) as c, avg(salary) as a, empid, deptno from emps where empid = 150 group by empid, deptno;
select count(*) as c, avg(salary) as a, empid, deptno from emps where empid = 150 group by empid, deptno order by empid, deptno;
