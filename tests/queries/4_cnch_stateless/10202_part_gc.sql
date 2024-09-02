drop table if exists t10202;

create table t10202 (i Int64) engine=CnchMergeTree order by i
settings old_parts_lifetime = 0, gc_ignore_running_transactions_for_test = 1, gc_trash_part_limit = 1;

system start gc t10202;
system stop gc t10202;
system start merges t10202;

insert into t10202 values (1);
insert into t10202 values (2);
optimize table t10202 final settings mutations_sync = 1, disable_optimize_final = 0;
-- test whether it will trash tombstone before covered parts
system gc t10202;
system drop cnch part cache t10202; -- simulate server restart with empty cache
select 'after part gc and drop cache';
select * from t10202 order by i;

-- test tombstone can be trashed when alone
alter table t10202 modify setting gc_trash_part_limit = 0;
system gc t10202; -- clear covered parts
select 'after first gc';
select '#tombstones', countIf(part_type = 'Tombstone') from system.cnch_parts where database = currentDatabase(1) and table = 't10202';
system gc t10202; -- clear alone tombstones
select 'after second gc';
select '#tombstones', countIf(part_type = 'Tombstone') from system.cnch_parts where database = currentDatabase(1) and table = 't10202';
select * from t10202 order by i;

-- test drop partition for range tombstone
insert into t10202 values (3);
insert into t10202 values (4);
alter table t10202 drop partition id 'all';
insert into t10202 values (5);
insert into t10202 values (6);
alter table t10202 drop partition id 'all';
select 'after insert and drop';
select * from t10202 order by i;
system gc t10202;
select 'after first gc';
select '#tombstones', countIf(part_type = 'Tombstone') from system.cnch_parts where database = currentDatabase(1) and table = 't10202';
system gc t10202;
select 'after second gc';
select '#tombstones', countIf(part_type = 'Tombstone') from system.cnch_parts where database = currentDatabase(1) and table = 't10202';

drop table if exists t10202;
