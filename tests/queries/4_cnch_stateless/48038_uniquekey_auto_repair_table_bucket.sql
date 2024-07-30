SELECT 'Auto repair by dedup task';

DROP TABLE IF EXISTS unique_auto_repair_table_bucket_level_dedup;
CREATE TABLE unique_auto_repair_table_bucket_level_dedup (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree() PARTITION BY toDate(event_time) CLUSTER BY id INTO 10 BUCKETS ORDER BY s UNIQUE KEY id 
SETTINGS partition_level_unique_keys=0, enable_bucket_level_unique_keys=1, check_duplicate_key=0, check_duplicate_key_interval=30, duplicate_auto_repair=1, duplicate_repair_interval=30;

set dedup_key_mode='append';
INSERT INTO unique_auto_repair_table_bucket_level_dedup VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);
INSERT INTO unique_auto_repair_table_bucket_level_dedup VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);

set dedup_key_mode='replace';
INSERT INTO unique_auto_repair_table_bucket_level_dedup VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);

SYSTEM SYNC REPAIR TASK unique_auto_repair_table_bucket_level_dedup;
SELECT * FROM unique_auto_repair_table_bucket_level_dedup ORDER BY event_time, id;


SELECT 'Auto repair by checker task';
DROP TABLE IF EXISTS unique_auto_repair_table_bucket_level_checker;
CREATE TABLE unique_auto_repair_table_bucket_level_checker (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree() PARTITION BY toDate(event_time) CLUSTER BY id INTO 10 BUCKETS ORDER BY s UNIQUE KEY id 
SETTINGS partition_level_unique_keys=0, enable_bucket_level_unique_keys=1, check_duplicate_key=1, check_duplicate_key_interval=30, duplicate_auto_repair=1, duplicate_repair_interval=30;

set dedup_key_mode='append';
INSERT INTO unique_auto_repair_table_bucket_level_checker VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);
INSERT INTO unique_auto_repair_table_bucket_level_checker VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);

SYSTEM SYNC REPAIR TASK unique_auto_repair_table_bucket_level_checker;
SELECT * FROM unique_auto_repair_table_bucket_level_checker ORDER BY event_time, id;


SELECT 'Auto repair big table by checker task';
DROP TABLE IF EXISTS unique_auto_repair_big_table_table_bucket_level_checker;
CREATE TABLE unique_auto_repair_big_table_table_bucket_level_checker (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree() PARTITION BY toDate(event_time) CLUSTER BY id INTO 10 BUCKETS ORDER BY s UNIQUE KEY id 
SETTINGS partition_level_unique_keys=0, enable_bucket_level_unique_keys=1, check_duplicate_key=1, check_duplicate_for_big_table=1, check_predicate='toDate(event_time)=\'2020-10-29\'', check_duplicate_key=1, check_duplicate_key_interval=30, duplicate_auto_repair=1, duplicate_repair_interval=30;

set dedup_key_mode='append';
INSERT INTO unique_auto_repair_big_table_table_bucket_level_checker VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);
INSERT INTO unique_auto_repair_big_table_table_bucket_level_checker VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);

SYSTEM SYNC REPAIR TASK unique_auto_repair_big_table_table_bucket_level_checker;
SELECT * FROM unique_auto_repair_big_table_table_bucket_level_checker ORDER BY event_time, id;


DROP TABLE IF EXISTS unique_auto_repair_table_bucket_level_dedup;
DROP TABLE IF EXISTS unique_auto_repair_table_bucket_level_checker;
DROP TABLE IF EXISTS unique_auto_repair_big_table_table_bucket_level_checker;

