
--  #Get deafult engine value
--let $DEFAULT_ENGINE = `select @@global.default_storage_engine`

-- Tests for Column list which requires utf8 output
--source include/have_partition.inc
-- set names utf8;
drop table if exists t1;
create table t1 (a varchar(2))
partition by list columns (a)
( partition p0 values in (0x81));

--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
show create table t1;
drop table t1;
create table t1 (a varchar(2))
partition by list columns (a)
( partition p0 values in (0x80));

--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
show create table t1;
drop table t1;

create table t1 (a varchar(1023))
partition by range columns(a)
( partition p0 values less than ('CZ'),
  partition p1 values less than ('CH'),
  partition p2 values less than ('D'));
insert into t1 values ('czz'),('chi'),('ci'),('cg');
select * from t1 where a between 'cg' AND 'ci';
drop table t1;

--
-- BUG#48163, Dagger in UCS2 not working as partition value
--
create table t1 (a varchar(2))
partition by list columns (a)
(partition p0 values in (0x2020),
 partition p1 values in (''));

--Replace default engine value with static engine string 
--replace_result $DEFAULT_ENGINE ENGINE
show create table t1;
insert into t1 values ('');
insert into t1 values (0x2020);
drop table t1;
