drop database if exists test_patition;
create database test_patition;
use test_patition;

DROP TABLE IF EXISTS test48040;

CREATE TABLE test_patition.test48040
(
    `event_name` String,
    `date` Date,
    `hour` String
)
ENGINE = CnchMergeTree
PARTITION BY (date, hour)
ORDER BY (event_name, date, hour);

insert into test_patition.test48040 values('1','2024-04-01','1')('1','2024-04-01','2')('1','2024-04-02','1')('2','2024-04-02','2')('2','2024-04-02','3');

set enable_optimizer = 1;

SELECT 'test_48040_qqq' FROM test_patition.test48040 format Null;
SELECT 'test_48040_www' FROM test_patition.test48040 WHERE date != '2024-04-12' and hour IN ('23', '24');
SELECT 'test_48040_eee' FROM test_patition.test48040 WHERE (date IN ('2024-04-11', '2024-04-12')) AND (hour IN ('23', '24'));
SELECT 'test_48040_rrr' FROM test_patition.test48040 where date = '2024-04-01' format Null;
SELECT 'test_48040_ttt' FROM test_patition.test48040 where date = '2024-04-01' and hour in ('2', '11') format Null;

-- Currently there is no way to proactively execute `SYSTEM FLUSH LOGS`
-- on other servers, so we just wait for logs to be flushed.
SYSTEM FLUSH LOGS;
SELECT sleepEachRow(3) from numbers(3) FORMAT Null;

select ProfileEvents['PrunedPartitions'] from system.query_log where lower(query) not like '%profileevents%' and lower(query) like '%test_48040_qqq%' and  (type = 'QueryFinish') limit 1;
select ProfileEvents['PrunedPartitions'] from system.query_log where lower(query) not like '%profileevents%' and lower(query) like '%test_48040_www%' and  (type = 'QueryFinish') limit 1;
select ProfileEvents['PrunedPartitions'] from system.query_log where lower(query) not like '%profileevents%' and lower(query) like '%test_48040_eee%' and  (type = 'QueryFinish') limit 1;
select ProfileEvents['PrunedPartitions'] from system.query_log where lower(query) not like '%profileevents%' and lower(query) like '%test_48040_rrr%' and  (type = 'QueryFinish') limit 1;
select ProfileEvents['PrunedPartitions'] from system.query_log where lower(query) not like '%profileevents%' and lower(query) like '%test_48040_ttt%' and  (type = 'QueryFinish') limit 1;

drop database if exists test_patition;
DROP TABLE IF EXISTS test48040;
