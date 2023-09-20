set enable_optimizer=1;

USE test;

DROP TABLE IF EXISTS test.emps_local;
DROP TABLE IF EXISTS test.depts_local;
DROP TABLE IF EXISTS test.locations_local;
DROP TABLE IF EXISTS test.mv_data_local;

DROP TABLE IF EXISTS test.emps;
DROP TABLE IF EXISTS test.depts;
DROP TABLE IF EXISTS test.locations;
DROP TABLE IF EXISTS test.mv_data;
DROP TABLE IF EXISTS test.mv_define;

CREATE TABLE emps_local(
    empid UInt32,
    deptno UInt32,
    name Nullable(String),
    salary Nullable(Float64),
    commission Nullable(UInt32)
) ENGINE = MergeTree()
PRIMARY KEY empid
ORDER BY empid;
CREATE TABLE emps AS emps_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), emps_local);

INSERT INTO emps VALUES (100, 10, 'Bill', 10000, 1000);
INSERT INTO emps VALUES (200, 20, 'Eric', 8000, 500);
INSERT INTO emps VALUES (150, 10, 'Sebastian', 7000, null);
INSERT INTO emps VALUES (110, 10, 'Theodore', 11500, 250);

CREATE TABLE depts_local(
      deptno UInt32,
      name Nullable(String)
) ENGINE = MergeTree()
PRIMARY KEY deptno
ORDER BY deptno;
CREATE TABLE depts AS depts_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), depts_local);

INSERT INTO depts VALUES (10, 'Sales');
INSERT INTO depts VALUES (30, 'Marketing');
INSERT INTO depts VALUES (40, 'HR');

CREATE TABLE locations_local(
    locationid UInt32,
    name Nullable(String)
) ENGINE = MergeTree()
PRIMARY KEY locationid
ORDER BY locationid;
CREATE TABLE locations AS locations_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), locations_local);

INSERT INTO locations VALUES (10, 'San Francisco');
INSERT INTO locations VALUES (20, 'San Diego');

-- 1.test projection rewrite
DROP TABLE IF EXISTS test.mv_data_local;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data_local ENGINE = MergeTree()
PRIMARY KEY deptno
ORDER BY deptno AS
select deptno, sum(salary), sum(commission), sum(k)
from (select deptno, salary, commission, 100 as k
      from emps)
group by deptno;
CREATE TABLE mv_data AS mv_data_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), mv_data_local);

DROP TABLE IF EXISTS test.mv_define;
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

select '1.2 test duplicate names';
explain select deptno, sum(salary) as salary, sum(commission) as commission
from (select deptno, salary, commission, 100 as k
      from emps)
group by deptno;

-- 2. test filter rewrite
DROP TABLE IF EXISTS test.mv_data_local;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data_local ENGINE = MergeTree()
PRIMARY KEY deptno
ORDER BY deptno AS
select deptno, empid, name from emps
where deptno = 10 or deptno = 20 or empid < 160;
CREATE TABLE mv_data AS mv_data_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), mv_data_local);

DROP TABLE IF EXISTS test.mv_define;
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
DROP TABLE IF EXISTS test.mv_data_local;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data_local ENGINE = MergeTree()
PRIMARY KEY deptno
ORDER BY deptno AS
select empid, deptno, count(*) as c, sum(empid) as s
from emps group by empid, deptno;
CREATE TABLE mv_data AS mv_data_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), mv_data_local);

DROP TABLE IF EXISTS test.mv_define;
CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select empid, deptno, count(*) as c, sum(empid) as s
from emps group by empid, deptno;

select '3. test rollup aggregate rewrite';
explain select count(*) + 1 as c, deptno from emps group by deptno;
select count(*) + 1 as c, deptno from emps group by deptno order by deptno;

select '3.1 test rollup redundant aggregate';
explain select count(*) + 1 as c,  count(*) + 2 as d,  count(*) + 3 as e, deptno from emps group by deptno;
select count(*) + 1 as c,  count(*) + 2 as d,  count(*) + 3 as e, deptno from emps group by deptno order by deptno;

-- 4. test rollup aggregate rewrite with agg state
DROP TABLE IF EXISTS test.mv_data_local;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data_local ENGINE = MergeTree()
PRIMARY KEY deptno
ORDER BY deptno AS
select empid, deptno, countState() as c, sumState(empid) as s
from emps group by empid, deptno;
CREATE TABLE mv_data AS mv_data_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), mv_data_local);

DROP TABLE IF EXISTS test.mv_define;
CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select empid, deptno, countState() as c, sumState(empid) as s
from emps group by empid, deptno;

select '4. test rollup aggregate rewrite';
explain select count(*) as c, deptno from emps group by deptno;
select count(*) as c, deptno from emps group by deptno order by deptno;

-- 5. test rollup aggregate rewrite with filter
DROP TABLE IF EXISTS test.mv_data_local;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data_local ENGINE = MergeTree()
PRIMARY KEY deptno
ORDER BY deptno AS
select empid, deptno, countState(*) as c, sumState(empid) as s, avgState(salary) as a
from emps
group by empid, deptno;
CREATE TABLE mv_data AS mv_data_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), mv_data_local);

DROP TABLE IF EXISTS test.mv_define;
CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select empid, deptno, countState(*) as c, sumState(empid) as s, avgState(salary) as a
from emps
group by empid, deptno;

select '5. test rollup aggregate rewrite with filter';
explain select count(*) as c, avg(salary) as a, empid, deptno from emps where empid = 150 group by empid, deptno;
select count(*) as c, avg(salary) as a, empid, deptno from emps where empid = 150 group by empid, deptno order by empid, deptno;

select '5.1 test rollup aggregate eliminate const grouping key';
explain select count(*) as c, avg(salary) as a, deptno from emps where empid = 150 group by deptno;
select count(*) as c, avg(salary) as a, deptno from emps where empid = 150 group by deptno order by deptno;

select '5.2 test rollup aggregate have at least 1 grouping key';
explain select count(*) as c, avg(salary) as a, empid from emps where empid = 150 group by empid;
select count(*) as c, avg(salary) as a, empid from emps where empid = 150 group by empid order by empid;

-- 6. test order by rewrite with multi mv';
DROP TABLE IF EXISTS test.mv_data_local;
DROP TABLE IF EXISTS test.mv_data;
CREATE TABLE mv_data_local ENGINE = MergeTree()
    PRIMARY KEY deptno
    ORDER BY deptno AS
select empid, deptno, count(*) as c, sum(empid) as s, avg(salary) as a
from emps_local
group by empid, deptno;
CREATE TABLE mv_data AS mv_data_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), mv_data_local);

DROP TABLE IF EXISTS test.mv_define_local;
CREATE MATERIALIZED VIEW mv_define_local to mv_data_local AS
select empid, deptno, count(*) as c, sum(empid) as s, avg(salary) as a
from emps_local
group by empid, deptno;

DROP TABLE IF EXISTS test.mv_define;
CREATE TABLE mv_define AS mv_define_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), mv_define_local);

select '6. test distributed table on local mv';
explain select count(*) as c, deptno from emps group by deptno;
select count(*) as c, deptno from emps group by deptno order by deptno;
