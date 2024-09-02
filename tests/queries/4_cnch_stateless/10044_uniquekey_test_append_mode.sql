DROP TABLE IF EXISTS test_unique_append_mode_helper;
DROP TABLE IF EXISTS test_unique_append_mode;
CREATE TABLE test_unique_append_mode (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree() PARTITION BY toDate(event_time) ORDER BY (s, id) PRIMARY KEY s UNIQUE KEY id;

SET enable_staging_area_for_write = 1, dedup_key_mode = 'append';
INSERT INTO test_unique_append_mode VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500); -- { serverError 36 }
SET enable_staging_area_for_write = 0;
INSERT INTO test_unique_append_mode (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500, 1);  -- { serverError 36 }

SELECT 'insert some records with dedup';
SET dedup_key_mode = 'replace';
INSERT INTO test_unique_append_mode VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200), ('2020-10-29 23:50:00', 10001, '10001B', 8, 800), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500);
INSERT INTO test_unique_append_mode VALUES ('2020-10-30 00:05:00', 10001, '10001A', 1, 100), ('2020-10-30 00:05:00', 10002, '10002A', 2, 200);
INSERT INTO test_unique_append_mode VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10001, '10002A', 2, 200);

SELECT event_time, id, s, m1, m2 FROM test_unique_append_mode ORDER BY event_time, id;
SELECT 'insert same records without dedup';
SET dedup_key_mode = 'append';

INSERT INTO test_unique_append_mode VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200), ('2020-10-29 23:50:00', 10001, '10001B', 8, 800), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500);
INSERT INTO test_unique_append_mode VALUES ('2020-10-30 00:05:00', 10001, '10001A', 1, 100), ('2020-10-30 00:05:00', 10002, '10002A', 2, 200);
INSERT INTO test_unique_append_mode VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10001, '10002A', 2, 200);

SELECT event_time, id, s, m1, m2 FROM test_unique_append_mode ORDER BY event_time, id;
SELECT 'system dedup for repair';
SYSTEM DEDUP test_unique_append_mode FOR REPAIR;
SELECT event_time, id, s, m1, m2 FROM test_unique_append_mode ORDER BY event_time, id;
CREATE TABLE test_unique_append_mode_helper (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree() PARTITION BY toDate(event_time) ORDER BY (s, id) PRIMARY KEY s UNIQUE KEY id;

SELECT '';
SELECT 'test insert select';
SET dedup_key_mode = 'append';
INSERT INTO test_unique_append_mode_helper SELECT * FROM test_unique_append_mode;
INSERT INTO test_unique_append_mode_helper SELECT '2020-10-29 23:40:00', number, '10001A', 5, 500 FROM system.numbers limit 10;
SELECT event_time, id, s, m1, m2 FROM test_unique_append_mode_helper ORDER BY event_time, id;

DROP TABLE IF EXISTS test_unique_append_mode_helper;
DROP TABLE IF EXISTS test_unique_append_mode;
