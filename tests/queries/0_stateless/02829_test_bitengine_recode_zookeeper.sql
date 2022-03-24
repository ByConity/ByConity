drop table if exists test.test_bitengine_recode_1 settings database_atomic_wait_for_drop_and_detach_synchronously=1;
drop table if exists test.test_bitengine_for_move settings database_atomic_wait_for_drop_and_detach_synchronously=1;

create table test.test_bitengine_recode_1 (p_date Date, tag_id Int64, uids BitMap64 BitEngineEncode)
engine = HaMergeTree('/clickhouse/tables/test/test_bitengine_recode_1', '1') partition by p_date order by tag_id settings index_granularity = 128, ha_queue_update_sleep_ms=500;

create table test.test_bitengine_for_move (p_date Date, tag_id Int64, uids BitMap64)
engine = MergeTree partition by p_date order by tag_id settings index_granularity = 128;

insert into table test.test_bitengine_recode_1 values ('2020-01-01', 1, [11]);
insert into table test.test_bitengine_recode_1 values ('2020-01-01', 2, [12]);
insert into table test.test_bitengine_recode_1 values ('2020-01-01', 3, [13]);
insert into table test.test_bitengine_for_move values ('2020-01-02', 1, [11]);

select sleep(1) Format Null;

alter table test.test_bitengine_for_move detach partition '2020-01-02';
alter table test.test_bitengine_recode_1 move partition '2020-01-02' from test.test_bitengine_for_move;
alter table test.test_bitengine_recode_1 bitengine recode partition '2020-01-02' from detach;
alter table test.test_bitengine_recode_1 attach partition '2020-01-02';

select sleep(3) Format Null;
select database, table, version, is_valid from system.bitengine where database = 'test' and table like 'test_bitengine_recode_1' order by database, table, version;

select p_date, tag_id, uids from test.test_bitengine_recode_1 order by p_date, tag_id;
select p_date, tag_id, bitmapToArrayWithDecode(uids, 'test', 'test_bitengine_recode_1', 'uids') as ids from test.test_bitengine_recode_1 order by p_date, tag_id;

insert into table test.test_bitengine_recode_1 values ('2020-01-01', 4, [11]);
insert into table test.test_bitengine_recode_1 values ('2020-01-01', 5, [12]);
insert into table test.test_bitengine_recode_1 values ('2020-01-01', 6, [13]);
insert into table test.test_bitengine_for_move values ('2020-01-02', 4, [11]);

select sleep(3) Format Null;

alter table test.test_bitengine_for_move detach partition '2020-01-02';
alter table test.test_bitengine_recode_1 move partition '2020-01-02' from test.test_bitengine_for_move;
alter table test.test_bitengine_recode_1 BITENGINE RECODE PARTITION '2020-01-02' from detach;
alter table test.test_bitengine_recode_1 attach partition '2020-01-02';

select database, table, version, is_valid from system.bitengine where database = 'test' and table like 'test_bitengine_recode_1' order by database, table, version;
select sleep(3) Format Null;

select p_date, tag_id, uids from test.test_bitengine_recode_1 order by p_date, tag_id;
select p_date, tag_id, bitmapToArrayWithDecode(uids, 'test', 'test_bitengine_recode_1', 'uids') as ids from test.test_bitengine_recode_1 order by p_date, tag_id;

select database, table, version, is_valid from system.bitengine where database = 'test' and table like 'test_bitengine_recode_1' order by database, table, version;

drop table if exists test.test_bitengine_recode_1 settings database_atomic_wait_for_drop_and_detach_synchronously=1;
drop table if exists test.test_bitengine_for_move settings database_atomic_wait_for_drop_and_detach_synchronously=1;