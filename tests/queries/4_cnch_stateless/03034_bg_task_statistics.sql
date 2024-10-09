SET disable_optimize_final = 0;
SET mutations_sync = 1;

DROP TABLE IF EXISTS 03034_bg_task_statistics;

CREATE TABLE 03034_bg_task_statistics (a Int, b Int, p Int) ENGINE = CnchMergeTree ORDER BY a PARTITION BY p SETTINGS old_parts_lifetime = 1, ttl_for_trash_items = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 1;
-- bg task statistics object will be created lazily
SYSTEM START MERGES 03034_bg_task_statistics;

-- wait bg task statistics object to be initialized
SELECT sleepEachRow(2) FROM numbers(10) FORMAT Null;

INSERT INTO 03034_bg_task_statistics SELECT number, number, number % 2 FROM numbers(100);

SELECT sleepEachRow(2) FROM numbers(5) FORMAT Null;

-- if initialization of bg_stats is scheduled after optimize query, may read some duplicated records from server_part_log.
SELECT partition_id, inserted_parts > 0, now() - last_insert_time < 180 FROM cnch(server, system.bg_task_statistics) WHERE database = currentDatabase(0) AND table = '03034_bg_task_statistics' ORDER BY partition_id;

DROP TABLE 03034_bg_task_statistics;
