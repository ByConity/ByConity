use test;
DROP TABLE IF EXISTS dwm_cprf_jarvis_anr_bg_task_hi;
DROP TABLE IF EXISTS dwm_cprf_jarvis_anr_bg_task_hi_local;

CREATE TABLE dwm_cprf_jarvis_anr_bg_task_hi_local
(
    `app_id` Int32,
    `current_time_ms` UInt64,
    `device_id` Int64
)
ENGINE = MergeTree
ORDER BY app_id
SETTINGS index_granularity = 8192;
 CREATE TABLE dwm_cprf_jarvis_anr_bg_task_hi
(
    `app_id` Int32,
    `current_time_ms` UInt64,
    `device_id` Int64
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'dwm_cprf_jarvis_anr_bg_task_hi_local', cityHash64(device_id));

WITH (
        SELECT countDistinct((device_id, current_time_ms)) AS count
        FROM dwm_cprf_jarvis_anr_bg_task_hi
    ) AS total
SELECT DISTINCT
    (device_id, current_time_ms),
    total AS count
FROM dwm_cprf_jarvis_anr_bg_task_hi
ORDER BY current_time_ms DESC;

SELECT DISTINCT
    toDateTime(current_time_ms) as time1,
    device_id as d1,
    device_id as d2
FROM dwm_cprf_jarvis_anr_bg_task_hi
ORDER BY time1 DESC;

explain stats = 0
SELECT DISTINCT
    toDateTime(current_time_ms) as time1,
    device_id as d1,
    device_id as d2
FROM dwm_cprf_jarvis_anr_bg_task_hi
ORDER BY time1 DESC limit 10 settings enable_optimizer=1;

DROP TABLE IF EXISTS dwm_cprf_jarvis_anr_bg_task_hi;
DROP TABLE IF EXISTS dwm_cprf_jarvis_anr_bg_task_hi_local;