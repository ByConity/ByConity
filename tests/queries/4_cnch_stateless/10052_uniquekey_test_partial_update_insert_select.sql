set enable_unique_partial_update = 1;

drop table if exists uniquekey_partial_update_insert_select_source_table;
drop table if exists uniquekey_partial_update_insert_select_target1;
drop table if exists uniquekey_partial_update_insert_select_target2;

------- test partial update insert select & check data count and sum
create table uniquekey_partial_update_insert_select_source_table (id Int32, a Int32) Engine=CnchMergeTree() order by id;
create table uniquekey_partial_update_insert_select_target1 (id Int32, a Int32) Engine=CnchMergeTree() order by id unique key id SETTINGS enable_unique_partial_update = 1, dedup_impl_version = 'dedup_in_write_suffix', dedup_pick_worker_algo = 'consistent_hash';
create table uniquekey_partial_update_insert_select_target2 (id Int32, a Int32) Engine=CnchMergeTree() order by id unique key id SETTINGS enable_unique_partial_update = 1, dedup_impl_version = 'dedup_in_txn_commit', dedup_pick_worker_algo = 'consistent_hash';

insert into uniquekey_partial_update_insert_select_source_table (id, a) select number, number from system.numbers limit 100000;
insert into uniquekey_partial_update_insert_select_target1 select * from uniquekey_partial_update_insert_select_source_table;
insert into uniquekey_partial_update_insert_select_target2 select * from uniquekey_partial_update_insert_select_source_table;
select 'test partial update insert select & check data count and sum, select1';
select count(), sum(a) from uniquekey_partial_update_insert_select_target1;
select count(), sum(a) from uniquekey_partial_update_insert_select_target2;

alter table uniquekey_partial_update_insert_select_target1 modify setting dedup_pick_worker_algo = 'random';
alter table uniquekey_partial_update_insert_select_target2 modify setting dedup_pick_worker_algo = 'random';
insert into uniquekey_partial_update_insert_select_target1 select * from uniquekey_partial_update_insert_select_source_table;
insert into uniquekey_partial_update_insert_select_target2 select * from uniquekey_partial_update_insert_select_source_table;
select 'test partial update insert select & check data count and sum, select2';
select count(), sum(a) from uniquekey_partial_update_insert_select_target1;
select count(), sum(a) from uniquekey_partial_update_insert_select_target2;

alter table uniquekey_partial_update_insert_select_target1 modify setting dedup_pick_worker_algo = 'sequential';
alter table uniquekey_partial_update_insert_select_target2 modify setting dedup_pick_worker_algo = 'sequential';
insert into uniquekey_partial_update_insert_select_target1 select * from uniquekey_partial_update_insert_select_source_table;
insert into uniquekey_partial_update_insert_select_target2 select * from uniquekey_partial_update_insert_select_source_table;
select 'test partial update insert select & check data count and sum, select3';
select count(), sum(a) from uniquekey_partial_update_insert_select_target1;
select count(), sum(a) from uniquekey_partial_update_insert_select_target2;

drop table if exists uniquekey_partial_update_insert_select_source_table;
drop table if exists uniquekey_partial_update_insert_select_target1;
drop table if exists uniquekey_partial_update_insert_select_target2;
