DROP TABLE IF EXISTS unique_partial_update_multi_round;

CREATE TABLE unique_partial_update_multi_round
(
    `p_date` Date,
    `id` UInt32,
    `number` UInt32,
    `content` String,
    `extra` String
)
ENGINE = CnchMergeTree
PARTITION BY p_date
ORDER BY id
UNIQUE KEY id
SETTINGS enable_unique_partial_update = 1, partial_update_max_process_parts = 1, partial_update_optimize_for_batch_task = 0;

SYSTEM STOP DEDUP WORKER unique_partial_update_multi_round;

SET enable_staging_area_for_write=1, enable_unique_partial_update=1;
INSERT INTO unique_partial_update_multi_round VALUES ('2023-01-02', 1005, 30, 'd2', 'x2');

SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
INSERT INTO unique_partial_update_multi_round VALUES ('2023-01-02', 1005, 30, 'd2', 'x2');

SELECT * FROM unique_partial_update_multi_round ORDER BY id;
DROP TABLE IF EXISTS unique_partial_update_multi_round;
