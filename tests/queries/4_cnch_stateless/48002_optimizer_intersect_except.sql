create database if not exists test_oie_48002;
use test_oie_48002;
set enable_optimizer=1;
set dialect_type='ANSI';
drop table if exists t1;
drop table if exists t2;

CREATE TABLE t1 (a Int32, b Nullable(Int32), c Nullable(String)) ENGINE = CnchMergeTree() PARTITION BY a ORDER BY a;
CREATE TABLE t2 (a Int32, b Nullable(Int32), c Nullable(String)) ENGINE = CnchMergeTree() PARTITION BY a ORDER BY a;

insert into t1 values(1, 1, '1')(2, 2, '2')(3, 2, '2')(4, 2, '3')(5, 3, '2')(6, 3, '2')(7, 4, '2')(8, null, '2');
insert into t2 values(1, 1, '1')(2, 2, '2')(3, 2, '2')(9, 7, '3')(10, 8, null)(11, null, '2');

select * from ( select b from t1 except all select b from t2 ) order by b;
select * from ( select b from t1 except select b from t2 ) order by b;
select * from ( select c from t1 except all select c from t2 ) order by c;
select * from ( select c from t1 except select c from t2 ) order by c;
select * from ( select b from t1 except all select a from t2 except all select b from t2 ) order by b;
select * from ( select b from t1 except all select a from t2 except select b from t2 ) order by b;
select * from ( select b from t1 except select a from t2 except all select b from t2 ) order by b;
select * from ( select b from t1 except select a from t2 except select b from t2 ) order by b;

select * from ( select a from t1 intersect all select a from t2 ) order by a;
select * from ( select a from t1 intersect select a from t2 ) order by a;
select * from ( select c from t1 intersect all select c from t2 ) order by c;
select * from ( select c from t1 intersect select c from t2 ) order by c;
select * from ( select a from t1 intersect all select a from t2 intersect all select a from t2 ) order by a;
select * from ( select a from t1 intersect all select a from t2 intersect select a from t2 ) order by a;
select * from ( select a from t1 intersect select a from t2 intersect all select a from t2 ) order by a;
select * from ( select a from t1 intersect select a from t2 intersect select a from t2 ) order by a;

set enable_setoperation_to_agg = 0;
select * from ( select b from t1 except all select a from t2 except all select b from t2 ) order by b;
select * from ( select b from t1 except all select a from t2 except select b from t2 ) order by b;
select * from ( select b from t1 except select a from t2 except all select b from t2 ) order by b;
select * from ( select b from t1 except select a from t2 except select b from t2 ) order by b;

select * from ( select a from t1 intersect all select a from t2 intersect all select a from t2 ) order by a;
select * from ( select a from t1 intersect all select a from t2 intersect select a from t2 ) order by a;
select * from ( select a from t1 intersect select a from t2 intersect all select a from t2 ) order by a;
select * from ( select a from t1 intersect select a from t2 intersect select a from t2 ) order by a;

drop table if exists t1;
drop table if exists t2;