set dedup_key_mode='append';

-- Test cnch_system.cnch_unique_table_log
DROP TABLE IF EXISTS unique_found_duplicate;
CREATE TABLE unique_found_duplicate (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(event_time) PARTITION BY toDate(event_time) ORDER BY (s, id) PRIMARY KEY s UNIQUE KEY id;

INSERT INTO unique_found_duplicate VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);
INSERT INTO unique_found_duplicate VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);

SELECT 'Select before found duplicate';
SELECT event_time, id, s, m1, m2 FROM unique_found_duplicate ORDER BY event_time, id;

set dedup_key_mode='replace';
INSERT INTO unique_found_duplicate VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);
-- Need to sleep for cnch_system.cnch_unique_table_log sync(currently 21 seconds, same as 10052_cnch_kafka_test)
SELECT 'Sleep 21 seconds for cnch_system.cnch_unique_table_log sync';
SELECT sleepEachRow(3) FROM numbers(7) FORMAT Null;

SELECT 'duplicate info:', table, event_type, has_error, event_msg from cnch_system.cnch_unique_table_log where table='unique_found_duplicate' and metric=7201 limit 1;

DROP TABLE IF EXISTS unique_found_duplicate;
