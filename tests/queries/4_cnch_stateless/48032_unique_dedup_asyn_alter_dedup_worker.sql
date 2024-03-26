select '-----------------------------------------------------';
select 'test enable staging area';

create database if not exists test_unique_dedup_asyn_alter_dedup_worker_db;
drop table if exists test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;

set enable_staging_area_for_write = 1;

CREATE table test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker(
    `event_time` DateTime,
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = CnchMergeTree(event_time)
partition by toDate(event_time)
CLUSTER BY product_id INTO 10 BUCKETS
order by (event_time, product_id)
unique key product_id
settings partition_level_unique_keys = 1,enable_bucket_level_unique_keys = 1,max_dedup_worker_number = 1;

SYSTEM STOP DEDUP WORKER test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;

insert into test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker values ('2021-07-13 18:50:00', 10001, 5, 500),('2021-07-13 18:50:00', 10002, 2, 200),('2021-07-13 18:50:00', 10003, 1, 100);
insert into test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker values ('2021-07-13 18:50:01', 10002, 4, 400),('2021-07-14 18:50:01', 10003, 2, 200),('2021-07-13 18:50:01', 10004, 1, 100);

alter table test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker modify setting max_dedup_worker_number = 10;

SYSTEM START DEDUP WORKER test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;

SYSTEM SYNC DEDUP WORKER test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;

select 'select unique table count()';
select count() from test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;
select * from test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker order by event_time,product_id,amount,revenue;

SYSTEM STOP DEDUP WORKER test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;

select 'test delete flag';
insert into test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker (event_time, product_id, amount, revenue, _delete_flag_) values ('2021-07-13 18:50:01', 10001, 5, 500, 1),('2021-07-14 18:50:00', 10002, 2, 200, 1);

SYSTEM START DEDUP WORKER test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;

alter table test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker modify setting max_dedup_worker_number = 2;

SYSTEM SYNC DEDUP WORKER test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;

select 'select unique table count()';
select count() from test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;
select * from test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker order by event_time,product_id,amount,revenue;

select 'select insert delete data';
insert into test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker (event_time, product_id, amount, revenue) select event_time, product_id, amount, revenue from test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker where revenue > 200;

SYSTEM SYNC DEDUP WORKER test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;

select 'select unique table count()';
select count() from test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;
select * from test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker order by event_time,product_id,amount,revenue;

select 'select delete more data';
insert into test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker (event_time, product_id, amount, revenue, _delete_flag_) select event_time, product_id, amount, revenue, 1 as _delete_flag_ from test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker where revenue > 0;

SYSTEM SYNC DEDUP WORKER test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;

select 'select unique table count()';
select count() from test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;
select * from test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker order by event_time,product_id,amount,revenue;

drop table if exists test_unique_dedup_asyn_alter_dedup_worker_db.unique_dedup_asyn_alter_dedup_worker;
drop database if exists test_unique_dedup_asyn_alter_dedup_worker_db;
