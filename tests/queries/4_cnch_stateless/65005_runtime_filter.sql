drop table if exists emps;
drop table if exists emps2;
drop table if exists emps_bucket;
drop table if exists emps_bucket2;

CREATE TABLE emps (empid  UInt32, deptno UInt32) ENGINE = CnchMergeTree() ORDER BY empid SETTINGS allow_nullable_key = 1;
CREATE TABLE emps2 (empid  UInt32, deptno UInt32) ENGINE = CnchMergeTree() ORDER BY empid SETTINGS allow_nullable_key = 1;
CREATE TABLE emps_bucket (empid  UInt32, deptno UInt32) ENGINE = CnchMergeTree() ORDER BY empid CLUSTER BY empid INTO 3 buckets SETTINGS allow_nullable_key = 1;
CREATE TABLE emps_bucket2 (empid  UInt32, deptno UInt32) ENGINE = CnchMergeTree() ORDER BY empid CLUSTER BY empid INTO 3 buckets SETTINGS allow_nullable_key = 1;

insert into emps (empid, deptno) select number, number % 10 from system.numbers limit 10000;
insert into emps2 (empid, deptno) select number % 10, number % 10 from system.numbers limit 100;
insert into emps_bucket (empid, deptno) select number, number % 10 from system.numbers limit 10000;
insert into emps_bucket2 (empid, deptno) select number % 10, number % 10 from system.numbers limit 100;

create stats emps Format Null;
create stats emps2 Format Null;
create stats emps_bucket Format Null;
create stats emps_bucket2 Format Null;

-- test distributed runtime filter and colocated runtime filter
set enable_optimizer=1;
select count() from emps e1 join emps e2 on e1.empid = e2.empid where e2.deptno = 0;
select count() from emps_bucket e1 join emps_bucket2 e2 on e1.empid = e2.empid where e2.deptno = 0;

-- test empty
select count() from emps e1 join emps e2 on e1.empid = e2.empid where e2.deptno = -1;
select count() from emps_bucket e1 join emps_bucket2 e2 on e1.empid = e2.empid where e2.deptno = -1;

-- test bypass
set runtime_filter_in_build_threshold=1;
set runtime_filter_bloom_build_threshold=1;
select count() from emps e1 join emps e2 on e1.empid = e2.empid where e2.deptno = 0;
select count() from emps_bucket e1 join emps_bucket2 e2 on e1.empid = e2.empid where e2.deptno = 0;
