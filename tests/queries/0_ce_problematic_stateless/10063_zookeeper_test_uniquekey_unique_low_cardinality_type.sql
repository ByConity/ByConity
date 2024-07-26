set database_atomic_wait_for_drop_and_detach_synchronously = 1;


drop table if exists test.unique_key_low_cardinality1;
drop table if exists test.unique_key_low_cardinality2;

create table test.unique_key_low_cardinality1 (c1 Int32, c2 Int32, k1 LowCardinality(UInt32), k2 LowCardinality(String), v1 UInt32)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_key_low_cardinality1', 'r1', v1)
order by (k1, intHash64(k1))
unique key (k1, sipHash64(k2))
sample by intHash64(k1)
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_disk_based_unique_key_index=1;

create table test.unique_key_low_cardinality2 (c1 Int32, c2 Int32, k1 LowCardinality(UInt32), k2 LowCardinality(String), v1 UInt32)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_key_low_cardinality2', 'r1', v1)
order by (k1, intHash64(k1))
unique key (intHash64(k1), k2)
sample by intHash64(k1)
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_disk_based_unique_key_index=0;

insert into test.unique_key_low_cardinality1 values (0, 100, 1, 'a', 1), (1, 101, 1, 'b', 1), (2, 102, 1, 'c', 1);
insert into test.unique_key_low_cardinality1 values (3, 103, 1, 'b', 1), (4, 104, 2, 'b', 1), (5, 105, 2, 'a', 1);
insert into test.unique_key_low_cardinality1 values (6, 106, 3, 'a', 1), (7, 107, 3, 'b', 1), (8, 108, 2, 'a', 1);

insert into test.unique_key_low_cardinality2 values (0, 100, 1, 'a', 1), (1, 101, 1, 'b', 1), (2, 102, 1, 'c', 1);
insert into test.unique_key_low_cardinality2 values (3, 103, 1, 'b', 1), (4, 104, 2, 'b', 1), (5, 105, 2, 'a', 1);
insert into test.unique_key_low_cardinality2 values (6, 106, 3, 'a', 1), (7, 107, 3, 'b', 1), (8, 108, 2, 'a', 1);

system sync replica test.unique_key_low_cardinality2;
select '-- using disk_based unique key index method --';
select * from test.unique_key_low_cardinality1 order by k1, k2;

select '';
select '-- using in-memory unique key index method --';
select * from test.unique_key_low_cardinality2 order by k1, k2;

drop table if exists test.unique_key_low_cardinality1;
drop table if exists test.unique_key_low_cardinality2;
