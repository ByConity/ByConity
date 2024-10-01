set statistics_return_row_count_if_empty=0;
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
set dialect_type='CLICKHOUSE';
set enable_optimizer=1;
drop database if exists cache_policy_45013;
create database cache_policy_45013;
use cache_policy_45013;

create table t1(x UInt32) Engine=CnchMergeTree order by x;
create table t2(x UInt32) Engine=CnchMergeTree order by x;
create table t3(x UInt32) Engine=CnchMergeTree order by x;
create table t4(x UInt32) Engine=CnchMergeTree order by x;

insert into t1 values(1);
insert into t2 values(2)(2);
insert into t3 values(3)(3)(3);
insert into t4 values(4)(4)(4)(4);

set create_stats_time_output = 0;
create stats t1;
create stats t2;
create stats t3;
create stats t4;

select '';
select '--- original catalog ---';
explain cost=0, verbose=0 select * from t1, t2, t3, t4 SETTINGS statistics_cache_policy='catalog';
show stats all in catalog;

select '';
select '--- original cache ---';
-- not loaded in cache, should be empty
explain cost=0, verbose=0 select * from t1, t2, t3, t4 SETTINGS statistics_cache_policy='cache';
show stats all in cache;

select '';
select '--- original normal ---';
-- this will flush cache
explain cost=0, verbose=0 select * from t1, t2, t3, t4;
-- so this won't be accurate
show stats all; 

select '';
select '--- original cache ---';
explain cost=0, verbose=0 select * from t1, t2, t3, t4 SETTINGS statistics_cache_policy='cache';
show stats all in cache;

select '';
select '--- manipulating ---';
-- normal drop
drop stats t1;
-- drop from cache
drop stats t2 in cache;
-- drop from catalog, keep cache
drop stats t3 in catalog;
-- drop from catalog using hack, which keeps cache
set enable_optimizer=0;
set dialect_type='CLICKHOUSE';
-- set insert_distributed_sync=1;
-- insert into system.optimizer_statistics(table_uuid, column_name, tag, value, _delete_flag_) 
--     select os.table_uuid, os.column_name, os.tag, os.value, 1 
--     from system.optimizer_statistics as os, system.tables as t 
--     where t.database=currentDatabase() and t.name='t4' and statistics_unique_key(t.database, t.name)=os.table_uuid;
select sleep(3) FORMAT Null;
set enable_optimizer=1;

select '';
select '--- cache ---';
set statistics_cache_policy='cache';
explain cost=0, verbose=0 select * from t1, t2, t3, t4;
show stats all in cache;

select '';
select '--- catalog ---';
select sleep(3) FORMAT Null;
set statistics_cache_policy='catalog';
explain cost=0, verbose=0 select * from t1, t2, t3, t4;
show stats all in catalog;

select '';
select '--- normal ---';
-- this will flush cache for t2
set statistics_cache_policy='default';
explain cost=0, verbose=0 select * from t1, t2, t3, t4;
-- so this won't be accurate
show stats all; 

select '';
select '--- normal patched ---';
-- drop from cache
drop stats t2 in cache;
show stats all; 

drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop database if exists cache_policy_45013;