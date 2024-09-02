drop database if exists test_dql_crossjoin;
create database if not exists test_dql_crossjoin;
use test_dql_crossjoin;
create table t01(`id` UInt64, `str_0` String,`date_0` Date)engine=CnchMergeTree order by id partition by date_0;
insert into t01 values(1,'aaa','2023-06-12'),(2,'bbb','2023-06-12'),(3,'aaa','2023-06-13'),(1,'ddd','2023-06-14');
insert into t01 values(5,'abc','2023-06-15'),(6,'def','2023-06-16');
create table t02(`id` UInt64, `str_0` String,`date_0` Date)engine=CnchMergeTree order by id partition by date_0;
insert into t02 values(100,'ppp','2023-06-12'),(101,'jkl','2023-06-12'),(102,'ghj','2023-06-13'),(103,'bnm','2023-06-14'),(6,'fgh','2023-06-17');
create table t03(`id` UInt64, `str_0` String,`date_0` Date)engine=CnchMergeTree order by id partition by date_0;
insert into t03 values(1000,'qwe','2023-06-15'),(1001,'wer','2023-06-16'),(1002,'ert','2023-06-17'),(1003,'rty','2023-06-18'),(100,'ppp','2023-06-12');
insert into t02 values(4,'dfg','2023-06-17');
insert into t03 values(5,'jkl','2023-06-16'),(1,'iop',today()),(2,'yui',today()),(4,'sdf',today());

set dialect_type='MYSQL';
set enable_optimizer=0;
select id,date_0 from t01 join t03 on t01.id=t03.id where t01.id not in (select id from t02) group by id,date_0 order by id desc, date_0 asc limit 10;
