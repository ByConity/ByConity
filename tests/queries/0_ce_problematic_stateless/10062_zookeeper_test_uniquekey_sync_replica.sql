set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.unique_sync_replica_1;
drop table if exists test.unique_sync_replica_2;

-- NOTE: set ha_unique_update_log_sleep_ms as a big value to stop the update log task
CREATE table test.unique_sync_replica_1 ( `d` Date, `k` UInt64, `v` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/test_unique_system_sync_replica', 'r1') partition by d unique key k order by k
SETTINGS ha_unique_update_log_sleep_ms=3600000, ha_unique_replay_log_sleep_ms=10;

CREATE table test.unique_sync_replica_2 (`d` Date, `k` UInt64, `v` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/test_unique_system_sync_replica', 'r2') partition by d unique key k order by k
SETTINGS ha_unique_update_log_sleep_ms=3600000, ha_unique_replay_log_sleep_ms=10, replicated_can_become_leader=0;

insert into test.unique_sync_replica_1 values ('2021-01-10', 0, 0);
insert into test.unique_sync_replica_1 values ('2021-01-10', 1, 1), ('2021-01-10', 2, 2),
insert into test.unique_sync_replica_1 values ('2021-01-10', 0, 10), ('2021-01-10', 1, 11);

-- Expect select to be empty since no logs are committed
select * from test.unique_sync_replica_2 order by k;

-- Apply sync replica
system sync replica test.unique_sync_replica_2;

select * from test.unique_sync_replica_2 order by k;

drop table if exists test.unique_sync_replica_1;
drop table if exists test.unique_sync_replica_2;
