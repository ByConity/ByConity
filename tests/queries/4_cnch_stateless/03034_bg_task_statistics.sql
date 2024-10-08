DROP TABLE IF EXISTS 03034_bg_task_statistics;

CREATE TABLE 03034_bg_task_statistics (a Int, b Int, p Int) ENGINE = CnchMergeTree ORDER BY a PARTITION BY p SETTINGS old_parts_lifetime = 1, ttl_for_trash_items = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 1;
-- bg task statistics object will be created lazily
SYSTEM START MERGES 03034_bg_task_statistics;

-- wait bg task statistics object to be initialized
SELECT sleepEachRow(1) FROM numbers(15) FORMAT Null;

INSERT INTO 03034_bg_task_statistics SELECT number, number, number % 2 FROM numbers(100);
INSERT INTO 03034_bg_task_statistics SELECT number, number, number % 2 FROM numbers(100);
INSERT INTO 03034_bg_task_statistics SELECT number, number, number % 2 FROM numbers(100);

SET disable_optimize_final = 0;
SET mutations_sync = 1;

OPTIMIZE TABLE 03034_bg_task_statistics FINAL;

-- waiting for phase 1 gc
SELECT sleepEachRow(1) FROM numbers(5) FORMAT Null;

SELECT partition_id, inserted_parts, merged_parts, removed_parts, now() - last_insert_time < 180 FROM cnch(server, system.bg_task_statistics) WHERE database = currentDatabase(0) AND table = '03034_bg_task_statistics' ORDER BY partition_id;

-- will drop bg task stats of partition 0
ALTER TABLE 03034_bg_task_statistics DROP PARTITION ID '0';

SELECT sleepEachRow(1) FROM numbers(3) FORMAT Null;

SELECT count(1) FROM cnch(server, system.bg_task_statistics) WHERE database = currentDatabase(0) AND table = '03034_bg_task_statistics' AND partition_id = '0';

INSERT INTO 03034_bg_task_statistics SELECT number, number, 0 FROM numbers(100);

SELECT partition_id, inserted_parts, merged_parts, removed_parts, now() - last_insert_time < 180 FROM cnch(server, system.bg_task_statistics) WHERE database = currentDatabase(0) AND table = '03034_bg_task_statistics' AND partition_id = '0';

DROP TABLE 03034_bg_task_statistics;
