set enable_optimizer=1;
set enforce_round_robin=1;
-- set enable_parallel_input_generator=1;
create database if not exists test;
drop table if exists test.table_misc;
drop table if exists test.daily_misc;
CREATE TABLE test.table_misc  (`hash_uid` UInt64, `event_date` Date) ENGINE = CnchMergeTree PARTITION BY (event_date) CLUSTER BY hash_uid INTO 100 BUCKETS ORDER BY (hash_uid);
CREATE TABLE test.daily_misc  (`p_date` Date, `hash_uid` UInt64) ENGINE = CnchMergeTree PARTITION BY (p_date) CLUSTER BY hash_uid INTO 100 BUCKETS ORDER BY (hash_uid) SAMPLE BY hash_uid;

explain select count(*) from test.table_misc t, test.daily_misc d where t.hash_uid = d.hash_uid and event_date = p_date;
explain select count(a) from (select *, hash_uid+hash_uid a from test.table_misc) t, test.daily_misc d where t.hash_uid = d.hash_uid and event_date = p_date;

drop table if exists test.table_misc;
drop table if exists test.daily_misc;
