drop table if exists test_global_gc1;
drop table if exists test_global_gc2;
drop snapshot if exists s10201_db_empty;
drop snapshot if exists s10201_db;
drop snapshot if exists s10201_gc1;

system clean trash table nonexist; -- { serverError 60 }

-- this snapshot should not affect global gc of future tables
create snapshot s10201_db_empty ttl 1 days;

create table test_global_gc1 (id Int64, s String) engine = CnchMergeTree order by id;
insert into test_global_gc1 select number, 'A' from numbers(3);

create table test_global_gc2 (id Int64, s String) engine = CnchMergeTree order by id;
insert into test_global_gc2 select number, 'B' from numbers(3);

-- test retention period
set cnch_data_retention_time_in_sec = 86400;
select name from system.cnch_tables_history where database = currentDatabase(0) and name like 'test_global_gc%' order by name;
select 'drop table';
drop table test_global_gc1;
select name from system.cnch_tables_history where database = currentDatabase(0) and name like 'test_global_gc%' order by name;
select 'clean trash';
system clean trash table test_global_gc1;
select name from system.cnch_tables_history where database = currentDatabase(0) and name like 'test_global_gc%' order by name;
select 'undrop table';
undrop table test_global_gc1;
select id, s from test_global_gc1 order by id;

-- test snapshot references
set cnch_data_retention_time_in_sec = 0;
select 'create snapshots and drop tables';
create snapshot s10201_db ttl 1 days;
create snapshot s10201_gc1 to test_global_gc1 ttl 1 days;
drop table test_global_gc1;
drop table test_global_gc2;
select name from system.cnch_tables_history where database = currentDatabase(0) and name like 'test_global_gc%' order by name;
select 'clean trash';
system clean trash table test_global_gc1; -- cannot clean because of snapshots
system clean trash table test_global_gc2; -- cannot clean because of snapshots
select name from system.cnch_tables_history where database = currentDatabase(0) and name like 'test_global_gc%' order by name;
select 'drop db snapshot';
drop snapshot s10201_db;
system clean trash table test_global_gc1; -- cannot clean because of snapshots
system clean trash table test_global_gc2; -- can clean
select name from system.cnch_tables_history where database = currentDatabase(0) and name like 'test_global_gc%' order by name;
select 'drop table snapshot';
drop snapshot s10201_gc1;
system clean trash table test_global_gc1; -- can clean
select name from system.cnch_tables_history where database = currentDatabase(0) and name like 'test_global_gc%' order by name;

-- test global gc finds correct snapshots
select 'create and drop';
create table test_global_gc1 (id Int64, s String) engine = CnchMergeTree order by id;
insert into test_global_gc1 select number, 'A' from numbers(3);
drop table test_global_gc1;
select 'create snapshot';
create snapshot s10201_gc1 ttl 1 days;
select 'create and drop again';
create table test_global_gc1 (id Int64, s String) engine = CnchMergeTree order by id;
insert into test_global_gc1 select number, 'AA' from numbers(3);
drop table test_global_gc1;
select name from system.cnch_tables_history where database = currentDatabase(0) and name like 'test_global_gc%' order by name;
-- ambiguous clean
system clean trash table test_global_gc1; -- { serverError 351 }
select 'undrop second table';
undrop table test_global_gc1;
select id, s from test_global_gc1 order by id;
select 'clean first table';
system clean trash table test_global_gc1; -- can clean
select name from system.cnch_tables_history where database = currentDatabase(0) and name like 'test_global_gc%' order by name;
select 'second table not affected';
select id, s from test_global_gc1 order by id;
select 'drop snapshot';
drop snapshot s10201_gc1;
select 'drop and clean second table';
drop table test_global_gc1;
system clean trash table test_global_gc1; -- can clean
select name from system.cnch_tables_history where database = currentDatabase(0) and name like 'test_global_gc%' order by name;

drop table if exists test_global_gc1;
drop table if exists test_global_gc2;
drop snapshot if exists s10201_db_empty;
