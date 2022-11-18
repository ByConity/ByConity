SET enable_optimizer=1;
set enable_sharding_optimize=1;
CREATE DATABASE IF NOT EXISTS test;
use test;
CREATE TABLE test.table_misc_local (`hash_uid` UInt64, `event_date` Date) ENGINE = MergeTree order by  event_date;
CREATE TABLE test.daily_misc_local (`p_date` Date, `hash_uid` UInt64) ENGINE =  MergeTree order by p_date;
CREATE TABLE test.daily_misc (`p_date` Date, `hash_uid` UInt64) ENGINE = Distributed('test_shard_localhost', 'test', 'daily_misc_local', cityHash64(p_date));
CREATE TABLE test.table_misc (`hash_uid` UInt64, `event_date` Date) ENGINE = Distributed('test_shard_localhost', 'test', 'table_misc_local', cityHash64(event_date));

explain select count(*) from table_misc t, daily_misc d where t.hash_uid = d.hash_uid and event_date = p_date group by d.p_date;

DROP TABLE test.table_misc_local;
DROP TABLE test.daily_misc_local;
DROP TABLE test.daily_misc;
DROP TABLE test.table_misc;