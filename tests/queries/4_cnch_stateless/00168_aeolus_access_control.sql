set enable_optimizer=1;
set materialized_view_consistency_check_method='NONE';

DROP TABLE IF EXISTS emps;

CREATE TABLE emps(empid UInt32, deptno UInt32, name Nullable(String), salary Nullable(Float64), commission Nullable(UInt32) ) ENGINE = CnchMergeTree() ORDER BY empid;

INSERT INTO emps VALUES (100, 10, 'Bill', 10000, 1000);
INSERT INTO emps VALUES (200, 20, 'Eric', 8000, 500);
INSERT INTO emps VALUES (150, 10, 'Sebastian', 7000, null);
INSERT INTO emps VALUES (110, 10, 'Theodore', 11500, 250);

SELECT count() FROM emps settings access_table_names = 'emps' ;

-- check origin table access
SELECT * FROM emps where empid = 100 settings access_table_names = 'system.one'; -- {serverError 291}

DROP TABLE IF EXISTS mv_define;
DROP TABLE IF EXISTS mv_data;

CREATE TABLE mv_data ENGINE = CnchMergeTree() ORDER BY deptno AS SELECT deptno, sum(salary), sum(commission), sum(k) FROM (SELECT deptno, salary, commission, 100 AS k FROM emps) GROUP BY deptno;
CREATE MATERIALIZED VIEW mv_define TO mv_data AS SELECT deptno, sum(salary), sum(commission), sum(k) FROM (SELECT deptno, salary, commission, 100 AS k FROM emps) GROUP BY deptno;

-- check optimizer rewrite with MV
SELECT deptno + 1, sum(salary) / 100, sum(k) FROM (SELECT deptno, salary, 100 AS k FROM emps) GROUP BY deptno ORDER BY deptno settings access_table_names = 'emps';
