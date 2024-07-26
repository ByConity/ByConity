drop database if exists 51004_partition_runtime_filter;
create database 51004_partition_runtime_filter;
use 51004_partition_runtime_filter;

set enable_optimizer=1;
set enable_optimizer_fallback=0;
set enable_partition_filter_push_down = 1;
-- set enable_early_partition_pruning = 1;

create table 51004_t1 (p_date Date, app_id Int32, id Int32) engine = CnchMergeTree
partition by (p_date, app_id) order by id;

create table 51004_t2 as 51004_t1;

insert into 51004_t1 select number % 5, number % 5, number from system.numbers limit 10000;

insert into 51004_t2 select * from 51004_t1 where id >= 0 and id < 10;

create stats 51004_t1 Format Null;
create stats 51004_t2 Format Null;

explain stats=0 select count() from 51004_t1 t1, 51004_t2 t2 where t1.p_date = t2.p_date and t2.id = 5;
select count() from 51004_t1 t1, 51004_t2 t2 where t1.p_date = t2.p_date and t2.id = 5;

drop database if exists 51004_partition_runtime_filter;