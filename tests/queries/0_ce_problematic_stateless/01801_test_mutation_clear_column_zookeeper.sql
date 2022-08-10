set database_atomic_wait_for_drop_and_detach_synchronously = 1;
SET mutations_sync = 2;
SET ha_alter_data_sync = 2;
SET ha_alter_metadata_sync = 2;
SET mutations_wait_timeout = 10;

drop table if exists test_mutation_1;
drop table if exists test_mutation_2;

CREATE TABLE test_mutation_1 (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32)) ENGINE = HaMergeTree('/clickhouse/test/tables/01801_test_mutation_clear_column_zookeeper', '1') PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192;
CREATE TABLE test_mutation_2 (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32)) ENGINE = HaMergeTree('/clickhouse/test/tables/01801_test_mutation_clear_column_zookeeper', '2') PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192, ha_queue_update_sleep_ms=1000;

insert into test_mutation_1 values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation_1 values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation_1 values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation_1 values ('2021-01-02', 2, 21, 'a', [1,2,3]);
insert into test_mutation_1 values ('2021-01-02', 3, 22, 'b', [1,2,3]);
insert into test_mutation_1 values ('2021-01-03', 3, 23, 'c', [1,2,3]);

select * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database = currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
SYSTEM SYNC REPLICA test_mutation_2;
select * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 clear column age in partition id '20210101';
alter table test_mutation_1 fastdelete age in partition '2021-01-02' where id = 2;
select 'clear column age in partition id 20210101';
select 'fast delete age in partition 2021-01-02 where id = 2';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database = currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 clear column age in partition where p_date = '2021-01-05';
select 'test clear column where command which will not affect any part';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database = currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 fastdelete age where id = 2;
alter table test_mutation_1 clear column ab in partition where p_date >= '2021-01-02';
select 'delete where id = 2';
select 'clear column ab in partition where p_date >= 2021-01-02';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database = currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

drop table test_mutation_1;
drop table test_mutation_2;

--- test tuple partition
select '';
select '--------------------------------------------------------------------';
select 'start to test tuple partition';
CREATE TABLE test_mutation_1 (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32)) ENGINE = HaMergeTree('/clickhouse/test/tables/01801_test_mutation_clear_column_zookeeper', '1') PARTITION BY (p_date, id) ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;
CREATE TABLE test_mutation_2 (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32)) ENGINE = HaMergeTree('/clickhouse/test/tables/01801_test_mutation_clear_column_zookeeper', '2') PARTITION BY (p_date, id) ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, ha_queue_update_sleep_ms=1000;

insert into test_mutation_1 values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation_1 values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation_1 values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation_1 values ('2021-01-02', 2, 21, 'a', [1,2,3]);
insert into test_mutation_1 values ('2021-01-02', 3, 22, 'b', [1,2,3]);
insert into test_mutation_1 values ('2021-01-03', 3, 23, 'c', [1,2,3]);

select * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database = currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
SYSTEM SYNC REPLICA test_mutation_2;
select * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 clear column age in partition id '20210101-1';
alter table test_mutation_1 fastdelete age in partition ('2021-01-02', 2) where age = 21;
select 'clear column age in partition id 20210101-1';
select 'fast delete age in partition (2021-01-02, 2) where age = 21';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database = currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 clear column age in partition where p_date = '2021-01-05';
select 'test clear column where command which will not affect any part';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database = currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 fastdelete age where id = 2;
alter table test_mutation_1 clear column ab in partition where p_date >= '2021-01-02';
select 'delete where id = 2';
select 'clear column ab in partition where p_date >= 2021-01-02';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database = currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

drop table test_mutation_1;
drop table test_mutation_2;
