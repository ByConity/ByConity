DROP TABLE IF EXISTS unique_partial_update_with_primary_index;

CREATE TABLE unique_partial_update_with_primary_index
(
    `p_date` Date,
    `id` UInt32,
    `number` UInt32,
    `content` String,
    `extra` String
)
ENGINE = CnchMergeTree()
PARTITION BY p_date
PRIMARY KEY id
ORDER BY id
UNIQUE KEY id
SETTINGS enable_unique_partial_update = 1;

SYSTEM STOP DEDUP WORKER unique_partial_update_with_primary_index;
SET enable_staging_area_for_write=1, enable_unique_partial_update=1;

INSERT INTO unique_partial_update_with_primary_index VALUES ('2023-01-02', 10001, 20, 'c1', 'e1');
INSERT INTO unique_partial_update_with_primary_index VALUES ('2023-01-02', 10002, 21, 'c2', 'e2');
INSERT INTO unique_partial_update_with_primary_index VALUES ('2023-01-02', 10003, 22, 'c3', 'e3');
INSERT INTO unique_partial_update_with_primary_index VALUES ('2023-01-02', 10004, 23, 'c4', 'e4');
INSERT INTO unique_partial_update_with_primary_index VALUES ('2023-01-02', 10005, 30, 'd2', 'x2');

SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
INSERT INTO unique_partial_update_with_primary_index (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1000, 30, 'd1', 'x3', 'extra');  

select * from unique_partial_update_with_primary_index order by id;
select id from unique_partial_update_with_primary_index where id=1000;

DROP TABLE IF EXISTS unique_partial_update_with_primary_index;
