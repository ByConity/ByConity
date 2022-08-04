set enable_optimizer=1;
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
CREATE TABLE test.table_misc  (`hash_uid` UInt64, `event_date` Date) ENGINE = CnchMergeTree PARTITION BY (event_date) CLUSTER BY hash_uid INTO 100 BUCKETS ORDER BY (hash_uid);
CREATE TABLE test.daily_misc  (`p_date` Date, `hash_uid` UInt64) ENGINE = CnchMergeTree PARTITION BY (p_date) CLUSTER BY hash_uid INTO 100 BUCKETS ORDER BY (hash_uid) SAMPLE BY hash_uid;
explain select count(*) from test.table_misc t, test.daily_misc d where t.hash_uid = d.hash_uid and event_date = p_date;