set enable_optimizer=1;
set materialized_view_consistency_check_method='NONE';
set enforce_materialized_view_rewrite=1;
set enable_optimizer_fallback=0;
set enable_optimizer_for_create_select=0;

DROP TABLE IF EXISTS mv_define;
DROP TABLE IF EXISTS mv_data;
DROP TABLE IF EXISTS emps;
DROP TABLE IF EXISTS depts;
DROP TABLE IF EXISTS locations;

CREATE TABLE emps(
    empid UInt32,
    deptno UInt32,
    name Nullable(String),
    salary Nullable(Float64),
    commission Nullable(UInt32)
) ENGINE = CnchMergeTree()
ORDER BY empid
partition by (empid, deptno);

INSERT INTO emps VALUES (100, 10, 'Bill', 10000, 1000);
INSERT INTO emps VALUES (200, 20, 'Eric', 8000, 500);
INSERT INTO emps VALUES (150, 10, 'Sebastian', 7000, null);
INSERT INTO emps VALUES (110, 10, 'Theodore', 11500, 250);

-- { echo }

-- 1.test projection rewrite
DROP TABLE IF EXISTS mv_define;
DROP TABLE IF EXISTS mv_data;
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

select deptno + 1, sum(salary) / 100, sum(k)
from (select deptno, salary, 100 as k
      from emps)
group by deptno
order by deptno;

select deptno + 1, sum(salary) / 100, sum(k)
from (select deptno, salary, 100 as k
      from emps)
group by deptno
order by deptno settings enable_materialized_view_rewrite = 0;;

-- 2. test duplicate names
select deptno, sum(salary) as salary, sum(commission) as commission
        from (select deptno, salary, commission, 100 as k
              from emps)
        group by deptno order by deptno;

select deptno, sum(salary) as salary, sum(commission) as commission
        from (select deptno, salary, commission, 100 as k
              from emps)
        group by deptno order by deptno settings enable_materialized_view_rewrite = 0;;

-- 3. test rollup aggregate rewrite
DROP TABLE IF EXISTS mv_define;
DROP TABLE IF EXISTS mv_data;
CREATE TABLE mv_data ENGINE = CnchMergeTree() ORDER BY deptno AS
select empid, deptno, count(*) as c, sum(empid) as s
from emps group by empid, deptno;

DROP TABLE IF EXISTS mv_define;
CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select empid, deptno, count(*) as c, sum(empid) as s
from emps group by empid, deptno;

select count(*) + 1 as c, deptno from emps group by deptno order by deptno;
select count(*) + 1 as c, deptno from emps group by deptno order by deptno settings enable_materialized_view_rewrite = 0;

-- 4. test rollup aggregate rewrite with agg state
DROP TABLE IF EXISTS mv_define;
DROP TABLE IF EXISTS mv_data;
CREATE TABLE mv_data ENGINE = CnchMergeTree() ORDER BY deptno AS
select empid, deptno, countState(*) as c, sumState(empid) as s
from emps group by empid, deptno;

DROP TABLE IF EXISTS mv_define;
CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select empid, deptno, countState(*) as c, sumState(empid) as s
from emps group by empid, deptno;

select count(*) as c, deptno from emps group by deptno order by deptno;
select count(*) as c, deptno from emps group by deptno order by deptno settings enable_materialized_view_rewrite = 0;

-- 5. test rollup aggregate rewrite with filter
DROP TABLE IF EXISTS mv_define;
DROP TABLE IF EXISTS mv_data;
CREATE TABLE mv_data ENGINE = CnchMergeTree() ORDER BY deptno AS
select empid, deptno, countState(*) as c, sumState(empid) as s, avgState(salary) as a
from emps
group by empid, deptno;

DROP TABLE IF EXISTS mv_define;
CREATE MATERIALIZED VIEW mv_define TO mv_data AS
select empid, deptno, countState(*) as c, sumState(empid) as s, avgState(salary) as a
from emps
group by empid, deptno;

select count(*) as c, avg(salary) as a, empid, deptno from emps where empid = 150 group by empid, deptno order by empid, deptno;
select count(*) as c, avg(salary) as a, empid, deptno from emps where empid = 150 group by empid, deptno order by empid, deptno settings enable_materialized_view_rewrite = 0;
