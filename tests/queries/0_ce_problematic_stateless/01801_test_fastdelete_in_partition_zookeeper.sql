SET mutations_sync = 2;
SET mutations_wait_timeout = 10;

drop table if exists test_mutation_1 sync;
drop table if exists test_mutation_2 sync;

CREATE TABLE test_mutation_1 (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32)) ENGINE = HaMergeTree('/clickhouse/tables/' || currentDatabase() || '/test_mutation', '1') PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192;
CREATE TABLE test_mutation_2 (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32)) ENGINE = HaMergeTree('/clickhouse/tables/' || currentDatabase() || '/test_mutation', '2') PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192, ha_queue_update_sleep_ms=1000;

insert into test_mutation_1 values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation_1 values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation_1 values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation_1 values ('2021-01-02', 2, 21, 'a', [1,2,3]);
insert into test_mutation_1 values ('2021-01-02', 3, 22, 'b', [1,2,3]);
insert into test_mutation_1 values ('2021-01-03', 3, 23, 'c', [1,2,3]);

select * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database=currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
SYSTEM SYNC REPLICA test_mutation_2;
select * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 fastdelete age in partition '2021-01-03' where id = 2;
select 'fast delete age in partition 2021-01-03 where id = 2';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database=currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 fastdelete age in partition id '2021-01-01' where id = 2;
select 'fast delete wrong usage: use partition name as partition id';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database=currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 fastdelete age in partition id '20210101' where id = 2;
select 'fast delete age in partition id 20210101 where id = 2';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database=currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 fastdelete age where id = 2;
select 'fast delete age where id = 2';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database=currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 fastdelete id in partition '2021-01-01' where event = 'c';
select 'fast delete id in partition 2021-01-01 where event = c';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database=currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

select '';
alter table test_mutation_1 fastdelete id where event = 'c';
select 'fast delete id where event = c';
select  * from test_mutation_1 order by p_date, id;
select 'show active part list';
select name from system.parts where database=currentDatabase() and table='test_mutation_1' and active order by name;
select 'select replica';
select  * from test_mutation_2 order by p_date, id;

drop table test_mutation_1 sync;
drop table test_mutation_2 sync;
