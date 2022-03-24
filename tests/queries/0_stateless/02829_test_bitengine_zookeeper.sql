drop table if exists test.test_bitengine_zk_1 settings database_atomic_wait_for_drop_and_detach_synchronously=1;
drop table if exists test.test_bitengine_zk_2 settings database_atomic_wait_for_drop_and_detach_synchronously=1;

create table test.test_bitengine_zk_1 (p_date Date, tag_id Int64, uids BitMap64 BitEngineEncode)
engine = HaMergeTree('/clickhouse/tables/test/test_bitengine_zk_123/{shard}', '1') partition by p_date order by tag_id settings index_granularity = 128, ha_queue_update_sleep_ms=500;
create table test.test_bitengine_zk_2 (p_date Date, tag_id Int64, uids BitMap64 BitEngineEncode)
engine = HaMergeTree('/clickhouse/tables/test/test_bitengine_zk_123/{shard}', '2') partition by p_date order by tag_id settings index_granularity = 128, ha_queue_update_sleep_ms=500;

insert into table test.test_bitengine_zk_1 values ('2020-01-01', 1, [11]);
insert into table test.test_bitengine_zk_1 values ('2020-01-01', 2, [12]);
insert into table test.test_bitengine_zk_1 values ('2020-01-01', 3, [13]);

select sleep(3) Format Null;

select database, table, version, is_valid from system.bitengine where database = 'test' and table like 'test_bitengine_zk_1' order by database, table, version;

select p_date, tag_id, uids from test.test_bitengine_zk_1 order by p_date, tag_id;
select p_date, tag_id, bitmapToArrayWithDecode(uids, 'test','test_bitengine_zk_1', 'uids') from test.test_bitengine_zk_1 order by p_date, tag_id;

insert into table test.test_bitengine_zk_1 values ('2020-01-01', 4, [11]);
insert into table test.test_bitengine_zk_1 values ('2020-01-01', 5, [12]);
insert into table test.test_bitengine_zk_1 values ('2020-01-01', 6, [13]);

select sleep(3) Format Null;

select database, table, version, is_valid from system.bitengine where database = 'test' and table like 'test_bitengine_zk_1' order by database, table, version;

select p_date, tag_id, uids from test.test_bitengine_zk_2 order by p_date, tag_id;
select p_date, tag_id, uids from test.test_bitengine_zk_2 order by p_date, tag_id settings use_encoded_bitmap=0;
select p_date, tag_id, bitmapToArrayWithDecode(uids, 'test','test_bitengine_zk_2', 'uids') from test.test_bitengine_zk_2 order by p_date, tag_id;

select database, table, version, is_valid from system.bitengine where database = 'test' and table like 'test_bitengine_zk_1' order by database, table, version;

drop table if exists test.test_bitengine_zk_1 settings database_atomic_wait_for_drop_and_detach_synchronously=1;
drop table if exists test.test_bitengine_zk_2 settings database_atomic_wait_for_drop_and_detach_synchronously=1;