select 'normal table, enable copy';

create table if not exists 03034_src_table (a int, b Date) engine = CnchMergeTree order by a partition by b;
create table if not exists 03034_dest_table (a int, b Date) engine = CnchMergeTree order by a partition by b;

insert into 03034_src_table values (1, '2024-08-14');
insert into 03034_dest_table values (2, '2024-08-14');

select 'attach/detach partition';
alter table 03034_src_table detach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1;
select count() from 03034_src_table where b = '2024-08-14';
alter table 03034_src_table attach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1;
select count() from 03034_src_table where b = '2024-08-14';

select 'attach partition from';
alter table 03034_dest_table attach partition '2024-08-14' from 03034_src_table settings cnch_enable_copy_for_partition_operation = 1;
select count() from 03034_src_table where b = '2024-08-14' and a = 1;
select count() from 03034_dest_table where b = '2024-08-14' and a = 1;
select count() from 03034_dest_table where b = '2024-08-14' and a = 2;

select 'replace partition from';
alter table 03034_dest_table replace partition '2024-08-14' from 03034_src_table settings cnch_enable_copy_for_partition_operation = 1;
select count() from 03034_src_table where b = '2024-08-14' and a = 1;
select count() from 03034_dest_table where b = '2024-08-14' and a = 1;
select count() from 03034_dest_table where b = '2024-08-14' and a = 2;

drop table if exists 03034_src_table;
drop table if exists 03034_dest_table;

select 'unique table, enable copy';
create table if not exists 03034_unique_src_table (a int, b Date) engine = CnchMergeTree order by a partition by b unique key a;
insert into 03034_unique_src_table (a, b, _delete_flag_) values (120,'2024-08-14',1), (119,'2024-08-14',1),(118,'2024-08-14',1),(117,'2024-08-14',1),(116,'2024-08-14',1),(115,'2024-08-14',1),(114,'2024-08-14',1),(113,'2024-08-14',1),(112,'2024-08-14',1),(111,'2024-08-14',1),(20,'2024-08-14',1),(19,'2024-08-14',1),(18,'2024-08-14',1),(17,'2024-08-14',1),(16,'2024-08-14',1),(15,'2024-08-14',1),(14,'2024-08-14',1),(13,'2024-08-14',1),(12,'2024-08-14',1),(11,'2024-08-14',0);

select 'attach/detach partition';
select count() from 03034_unique_src_table where a = 11 and b = '2024-08-14';
alter table 03034_unique_src_table detach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1;
select count() from 03034_unique_src_table where b = '2024-08-14';
alter table 03034_unique_src_table attach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1;
select count() from 03034_unique_src_table where b = '2024-08-14' and a = 11;

drop table if exists 03034_unique_src_table;

-- 'throw exceptions'
create table if not exists 03034_exception_src_normal_table (a int, b Date) engine = CnchMergeTree order by a partition by b cluster by a INTO 10 BUCKETS;
create table if not exists 03034_exception_dest_normal_table (a int, b Date) engine = CnchMergeTree order by a partition by b cluster by a INTO 10 BUCKETS;
create table if not exists 03034_exception_src_unique_table (a int, b Date) engine = CnchMergeTree order by a partition by b unique key a;
create table if not exists 03034_exception_dest_unique_table (a int, b Date) engine = CnchMergeTree order by a partition by b unique key a;

alter table 03034_exception_dest_normal_table attach detached partition '2024-08-14' from 03034_exception_src_normal_table settings cnch_enable_copy_for_partition_operation = 1; -- { serverError 48 }
alter table 03034_exception_src_normal_table attach partition '2024-08-14' from '/home/byte_dp_cnch_lf/cicd/fake_directory' settings cnch_enable_copy_for_partition_operation = 1; -- { serverError 48 }
alter table 03034_exception_src_normal_table attach partition '2024-08-14' bucket 1 settings cnch_enable_copy_for_partition_operation = 1; -- { serverError 48 }
insert into 03034_exception_src_normal_table select number, '2024-08-14' from system.numbers limit 100;
alter table 03034_exception_src_normal_table detach partition '2024-08-14' bucket 1 settings cnch_enable_copy_for_partition_operation = 1; -- { serverError 48 }

alter table 03034_exception_dest_unique_table attach partition '2024-08-14' from 03034_exception_src_unique_table; -- { serverError 48 }
alter table 03034_exception_dest_unique_table attach detached partition '2024-08-14' from 03034_exception_src_unique_table;
alter table 03034_exception_dest_unique_table replace partition '2024-08-14' from 03034_exception_src_unique_table; -- { serverError 48 }
-- generate staged partition for unique table
set enable_wait_attached_staged_parts_to_visible = 0, enable_unique_table_attach_without_dedup = 0;
insert into 03034_exception_src_unique_table select number, '2024-08-14' from system.numbers limit 100;
system stop dedup worker 03034_exception_src_unique_table;
alter table 03034_exception_src_unique_table detach partition '2024-08-14';
alter table 03034_exception_src_unique_table attach partition '2024-08-14';
alter table 03034_exception_src_unique_table detach staged partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1; -- { serverError 48 }

-- 'not throw exceptions'
alter table 03034_exception_dest_normal_table attach detached partition '2024-08-14' from 03034_exception_src_normal_table;
alter table 03034_exception_src_normal_table attach partition '2024-08-14' bucket 1;
alter table 03034_exception_src_normal_table detach partition '2024-08-14' bucket 1;

alter table 03034_exception_src_unique_table detach staged partition '2024-08-14';

drop table if exists 03034_exception_src_normal_table;
drop table if exists 03034_exception_dest_normal_table;
drop table if exists 03034_exception_src_unique_table;
drop table if exists 03034_exception_dest_unique_table;

select 'part chain detach/attach';
create table if not exists 03034_chain_src_normal_table (a int, b Date, c int, d int) engine = CnchMergeTree order by a partition by b;
create table if not exists 03034_chain_dest_normal_table (a int, b Date, c int, d int) engine = CnchMergeTree order by a partition by b;
system stop merges 03034_chain_src_normal_table;
system start merges 03034_chain_src_normal_table;

insert into 03034_chain_src_normal_table values (1, '2024-08-14', 1, 10);
select count() from 03034_chain_src_normal_table where a = 1 and b = '2024-08-14' and c = 1 and d = 10;
alter table 03034_chain_src_normal_table update c = 2 where a = 1 settings mutations_sync = 1;
alter table 03034_chain_src_normal_table update d = 20 where a = 1 settings mutations_sync = 1;
select count() from 03034_chain_src_normal_table where a = 1 and b = '2024-08-14' and c = 2 and d = 20;
alter table 03034_chain_src_normal_table detach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1;
alter table 03034_chain_src_normal_table attach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1;
alter table 03034_chain_src_normal_table detach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1;
alter table 03034_chain_src_normal_table attach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1;
select count() from 03034_chain_src_normal_table where a = 1 and b = '2024-08-14' and c = 2 and d = 20;
alter table 03034_chain_dest_normal_table attach partition '2024-08-14' from 03034_chain_src_normal_table settings cnch_enable_copy_for_partition_operation = 1;
select count() from 03034_chain_src_normal_table where a = 1 and b = '2024-08-14' and c = 2 and d = 20;
select count() from 03034_chain_dest_normal_table where a = 1 and b = '2024-08-14' and c = 2 and d = 20;

drop table if exists 03034_chain_src_normal_table;
drop table if exists 03034_chain_dest_normal_table;

select '';
select 'part chain detach/attach for unique table';
set enable_wait_attached_staged_parts_to_visible = 1, enable_unique_table_attach_without_dedup = 0;
drop table if exists 03034_chain_src_unique_table;
create table if not exists 03034_chain_src_unique_table (a int, b Date, c int, d int) engine = CnchMergeTree order by a partition by b unique key a;
system stop merges 03034_chain_src_unique_table;
system start merges 03034_chain_src_unique_table;

insert into 03034_chain_src_unique_table (a, b, c, d, _delete_flag_) values (120,'2024-08-14',1,10,1), (119,'2024-08-14',1,10,1),(118,'2024-08-14',1,10,1),(117,'2024-08-14',1,10,1),(116,'2024-08-14',1,10,1),(115,'2024-08-14',1,10,1),(114,'2024-08-14',1,10,1),(113,'2024-08-14',1,10,1),(112,'2024-08-14',1,10,1),(111,'2024-08-14',1,10,1),(20,'2024-08-14',1,10,1),(19,'2024-08-14',1,10,1),(18,'2024-08-14',1,10,1),(17,'2024-08-14',1,10,1),(16,'2024-08-14',1,10,1),(15,'2024-08-14',1,10,1),(14,'2024-08-14',1,10,1),(13,'2024-08-14',1,10,1),(12,'2024-08-14',1,10,1),(11,'2024-08-14',1,10,0);
select * from 03034_chain_src_unique_table;
alter table 03034_chain_src_unique_table update c = 2 where a = 11 settings mutations_sync = 1;
alter table 03034_chain_src_unique_table update d = 20 where a = 11 settings mutations_sync = 1;
select * from 03034_chain_src_unique_table;
alter table 03034_chain_src_unique_table detach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 0;
alter table 03034_chain_src_unique_table attach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 0;
select * from 03034_chain_src_unique_table;
alter table 03034_chain_src_unique_table detach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1;
alter table 03034_chain_src_unique_table attach partition '2024-08-14' settings cnch_enable_copy_for_partition_operation = 1;
select * from 03034_chain_src_unique_table;
drop table if exists 03034_chain_src_unique_table;
