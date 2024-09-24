set mutations_sync = 1;
drop snapshot if exists s1;
drop snapshot if exists s2;
drop snapshot if exists s3;
drop snapshot if exists s4;
drop snapshot if exists s5;
drop table if exists t1;
drop table if exists t2;

create table t1 (d Date, i Int64, s String) engine = CnchMergeTree partition by d order by i
settings old_parts_lifetime = 0, gc_ignore_running_transactions_for_test = 1, ttl_for_trash_items = 0;
create table t2 (d Date, i Int64, s String) engine = CnchMergeTree partition by d order by i unique key i
settings old_parts_lifetime = 0, gc_ignore_running_transactions_for_test = 1, ttl_for_trash_items = 0;

-- prefer manual gc in test, disable bg thread
system start gc t1;
system stop gc t1;
system start gc t2;
system stop gc t2;
-- to execute merge & mutation
system start merges t1;
system start merges t2;

select 'create s1';
create snapshot s1 to t1 ttl 1 days;
create snapshot if not exists s1 to t1 ttl 1 days; -- no op
select name, table_uuid is not null, ttl_in_days from system.cnch_snapshots where database = currentDatabase(0) and name = 's1';

create snapshot s1 to t1 ttl 1 days; -- { serverError 1150 }
create snapshot bad to unknown_table ttl 1 days; -- { serverError 60 }

insert into t1 values ('2023-10-01', 1, '1');
insert into t1 values ('2023-10-01', 2, '2');
insert into t1 values ('2023-10-01', 3, '3');

-- test snapshot and drop partition
select 'create s2';
create snapshot s2 to t1 ttl 1 days;
select 'drop 20231001';
alter table t1 drop partition id '20231001';
insert into t1 values ('2023-10-01', 4, '4');
insert into t1 values ('2023-10-01', 5, '5');
select * from t1 order by d, i settings use_snapshot='';
system gc t1;
select * from t1 order by d, i settings use_snapshot='nonexist'; -- { serverError 1151 }
select 'select use s1';
select * from t1 order by d, i settings use_snapshot='s1';
select 'select use s2';
select * from t1 order by d, i settings use_snapshot='s2';

-- test snapshot and merge
select 'create s3';
create snapshot s3 to t1 ttl 1 days;
select 'insert and optimize';
insert into t1 values ('2023-10-01', 6, '6');
insert into t1 values ('2023-10-01', 7, '7');
insert into t1 values ('2023-10-01', 8, '8');
optimize table t1;
select '#part', count(distinct _part) from t1;
system gc t1;
select 'select use s3';
select * from t1 order by d, i settings use_snapshot='s3';

-- test drop snapshot and gc
drop snapshot nonexist; -- { serverError 1151 }
drop snapshot if exists nonexist;
select 'drop s2, s3';
drop snapshot s2;
drop snapshot s3;
system gc t1;
select count(), countIf(visible), countIf(outdated) from system.cnch_parts where database = currentDatabase(1) and table = 't1';

-- test snapshot and mutation
select 'delete i = 4';
alter table t1 delete where i = 4;
select 'create s4';
create snapshot s4 to t1 ttl 1 days;
select 'delete i = 6';
alter table t1 delete where i = 6;
system gc t1;
select * from t1 order by d, i;
select 'select use s4';
select * from t1 order by d, i settings use_snapshot='s4';
drop snapshot s1;
drop snapshot s4;

-- test snapshot on t1 doesn't affect t2
select 'insert into t2';
insert into t2 select '2023-11-01', number, toString(number) from numbers(100);
select 'create s5 to t1';
create snapshot s5 to t1 ttl 1 days;
select 't2 drop 20231101';
alter table t2 drop partition id '20231101';
select * from t2 order by d, i settings use_snapshot='s5'; -- { serverError 1152 }
system gc t2; -- normal parts should be cleared
select count() from system.cnch_parts where database = currentDatabase(1) and table = 't2'; -- expect only one tombstone part
select 'drop s5';
drop snapshot s5;

-- test snapshot and delete bitmaps
select 'upsert t2';
insert into t2 select '2023-11-01', number, 'A' from numbers(100);
insert into t2 select '2023-11-01', number, 'B' from numbers(100) where number >= 90;
insert into t2 select '2023-11-01', number, 'C' from numbers(100) where number < 10;
select s, count(), sum(i) from t2 group by s order by s;
select 'create s1 to t2';
create snapshot s1 to t2 ttl 1 days;
insert into t2 select '2023-11-01', number, 'D' from numbers(100) where number >= 10 and number < 20;
select 'create s2 to t2';
create snapshot s2 to t2 ttl 1 days;
insert into t2 select '2023-11-01', number, 'E' from numbers(100) where number >= 20 and number < 50;
select 'create s3 to t2';
create snapshot s3 to t2 ttl 1 days;
insert into t2 select '2023-11-01', number, 'F' from numbers(100) where number < 10;
optimize table t2;
system gc t2;
select 'select use s1';
select s, count(), sum(i) from t2 group by s order by s settings use_snapshot = 's1';  --skip_if_readonly_ci
select 'select use s2';
select s, count(), sum(i) from t2 group by s order by s settings use_snapshot = 's2';  --skip_if_readonly_ci
select 'select use s3';
select s, count(), sum(i) from t2 group by s order by s settings use_snapshot = 's3';  --skip_if_readonly_ci
select 'select without snapshot';
select s, count(), sum(i) from t2 group by s order by s;
select 'drop all snapshots';
drop snapshot s1;
drop snapshot s2;
drop snapshot s3;
system gc t2;
select count() from system.cnch_trash_items where database = 'test' and table = 't2'; -- expect all trash items to be removed

drop snapshot if exists s1;
drop snapshot if exists s2;
drop snapshot if exists s3;
drop snapshot if exists s4;
drop snapshot if exists s5;
drop table if exists t1;
drop table if exists t2;
