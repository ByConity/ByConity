set enable_staging_area_for_write = 1;

-- Test cancel dedup worker
DROP TABLE IF EXISTS unique_cancel_dedup;
CREATE TABLE unique_cancel_dedup (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(event_time) PARTITION BY toDate(event_time) ORDER BY (s, id) PRIMARY KEY s UNIQUE KEY id SETTINGS disable_dedup_parts = 1;

SYSTEM START DEDUP WORKER unique_cancel_dedup;

INSERT INTO unique_cancel_dedup VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);

SELECT 'Select before cancel dedup worker';
SELECT event_time, id, s, m1, m2 FROM unique_cancel_dedup ORDER BY event_time, id;

-- Need to sleep for dedup worker's iterate(currently 9 seconds)
SELECT 'Sleep 9 seconds for dedup worker iterate';
SELECT sleepEachRow(3) FROM numbers(3) FORMAT Null;
SELECT 'Get dedup progress';

-- Currently, cnch_dedup_workers can only query the DedupWorkerManager on the current server. Consider aligning with cnch-1.4 in the future
SELECT 'dedup worker progress:', table, is_active, dedup_tasks_progress from cnch(server, system.cnch_dedup_workers) where table='unique_cancel_dedup';

SELECT 'Cancel dedup task';
SYSTEM STOP DEDUP WORKER unique_cancel_dedup;
SELECT 'Select after cancel dedup worker';
SELECT event_time, id, s, m1, m2 FROM unique_cancel_dedup ORDER BY event_time, id;

DROP TABLE IF EXISTS unique_cancel_dedup;
