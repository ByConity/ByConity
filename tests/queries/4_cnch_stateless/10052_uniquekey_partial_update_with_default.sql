DROP TABLE IF EXISTS unique_partial_update_with_default;

CREATE TABLE unique_partial_update_with_default
(
    `p_date` Date,
    `id` UInt32,
    `number` UInt32,
    `content` String,
    `extra` String DEFAULT concat(content, 'test')
)
ENGINE = CnchMergeTree()
PARTITION BY p_date
ORDER BY id
UNIQUE KEY id
SETTINGS enable_unique_partial_update = 1;

SYSTEM STOP DEDUP WORKER unique_partial_update_with_default;
SET enable_staging_area_for_write=0, enable_unique_partial_update=0;
INSERT INTO unique_partial_update_with_default VALUES ('2023-01-01', 1001, 20, 'c1', 'e1');
INSERT INTO unique_partial_update_with_default VALUES ('2023-01-01', 1002, 21, 'c2', 'e2');
INSERT INTO unique_partial_update_with_default VALUES ('2023-01-01', 1003, 22, 'c3', 'e3');
INSERT INTO unique_partial_update_with_default VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');

SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
-- data for unique key 1004: 2023-01-02, 1004, 23, d1, e4
-- data for unique key 1005: 2023-01-02, 1005, 0, d1, d1test
INSERT INTO unique_partial_update_with_default (p_date, id, content) values ('2023-01-02', 1004, 'd1'), ('2023-01-02', 1005, 'd1'); 
SELECT * FROM unique_partial_update_with_default ORDER BY id;

DROP TABLE IF EXISTS unique_partial_update_with_default;
