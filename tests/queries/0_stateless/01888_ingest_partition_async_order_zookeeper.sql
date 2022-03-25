set database_atomic_wait_for_drop_and_detach_synchronously = 1;

drop table if exists test.test_ingest_async_order_source1;
drop table if exists test.test_ingest_async_order_source2;
drop table if exists test.test_ingest_async_order_target1;
drop table if exists test.test_ingest_async_order_target2;

set enable_async_ingest = 1;

CREATE TABLE test.test_ingest_async_order_target1 (`p_date` Date, `uid` Int32, `event` String, `name` String) ENGINE = HaMergeTree('/clickhouse/test/test/test_ingest_async_order_test', '1') PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;
CREATE TABLE test.test_ingest_async_order_source1 (`p_date` Date, `uid` Int32, `event` String, `name` String) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;
CREATE TABLE test.test_ingest_async_order_source2 (`p_date` Date, `uid` Int32, `event` String, `name` String) ENGINE = MergeTree PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;

insert into test.test_ingest_async_order_target1 values ('2021-01-01', 1, 'a', 'b'), ('2021-01-01', 2, 'a', 'b');
insert into test.test_ingest_async_order_source1 values ('2021-01-01', 1, 'c', 'b'), ('2021-01-01', 2, 'c', 'b');
insert into test.test_ingest_async_order_source2 values ('2021-01-01', 1, 'd', 'b'), ('2021-01-01', 2, 'd', 'b');

CREATE TABLE test.test_ingest_async_order_target2 (`p_date` Date, `uid` Int32, `event` String, `name` String) ENGINE = HaMergeTree('/clickhouse/test/test/test_ingest_async_order_test', '2') PARTITION BY p_date ORDER BY uid SETTINGS index_granularity = 8192;

select sleep(1.1) format Null;
alter table test.test_ingest_async_order_target1 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source1;
alter table test.test_ingest_async_order_target1 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source2;

select sleep(1.5) format Null;
select * from test.test_ingest_async_order_target1 order by uid;

alter table test.test_ingest_async_order_target1 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source1;
alter table test.test_ingest_async_order_target2 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source2;

select sleep(1.5) format Null;
select * from test.test_ingest_async_order_target1 order by uid;

alter table test.test_ingest_async_order_target1 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source1;
alter table test.test_ingest_async_order_target2 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source2;
alter table test.test_ingest_async_order_target1 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source1;

select sleep(1.5) format Null;
select * from test.test_ingest_async_order_target1 order by uid;

alter table test.test_ingest_async_order_target1 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source1;
alter table test.test_ingest_async_order_target2 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source2;
alter table test.test_ingest_async_order_target1 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source1;
alter table test.test_ingest_async_order_target2 ingest partition '2021-01-01' columns event, name key uid from test.test_ingest_async_order_source2;

select sleep(1.5) format Null;
select * from test.test_ingest_async_order_target1 order by uid;

drop table if exists test.test_ingest_async_order_source1;
drop table if exists test.test_ingest_async_order_source2;
drop table if exists test.test_ingest_async_order_target1;
drop table if exists test.test_ingest_async_order_target2;