set enable_optimizer=1;

drop table if exists table_misc;
drop table if exists daily_misc;
CREATE TABLE table_misc  (`hash_uid` UInt64, `event_date` Date) ENGINE = CnchMergeTree PARTITION BY (event_date) CLUSTER BY hash_uid INTO 100 BUCKETS ORDER BY (hash_uid);
CREATE TABLE daily_misc  (`p_date` Date, `hash_uid` UInt64) ENGINE = CnchMergeTree PARTITION BY (p_date) CLUSTER BY hash_uid INTO 100 BUCKETS ORDER BY (hash_uid) SAMPLE BY hash_uid;
explain select count(*) from table_misc t, daily_misc d where t.hash_uid = d.hash_uid and event_date = p_date;
explain select count(a) from (select *, hash_uid+hash_uid a from table_misc) t, daily_misc d where t.hash_uid = d.hash_uid and event_date = p_date;
select count(*) from table_misc t, daily_misc d where t.hash_uid = d.hash_uid and event_date = p_date group by d.p_date settings enum_repartition=0;
drop table if exists table_misc;
drop table if exists daily_misc;
