DROP TABLE IF EXISTS unique_partial_update_insert_on_duplicate_key;

CREATE TABLE unique_partial_update_insert_on_duplicate_key
(
    `p_date` Date,
    `id` UInt32,
    `number` UInt32,
    `content` String,
    `extra` String
)
ENGINE = CnchMergeTree()
PARTITION BY p_date
ORDER BY id
UNIQUE KEY id
SETTINGS enable_unique_partial_update = 1;

SYSTEM STOP DEDUP WORKER unique_partial_update_insert_on_duplicate_key;

SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
INSERT INTO unique_partial_update_insert_on_duplicate_key VALUES ('2023-01-01', 1001, 20, 'c1', 'e1');
INSERT INTO unique_partial_update_insert_on_duplicate_key VALUES ('2023-01-01', 1002, 21, 'c2', 'e2');
INSERT INTO unique_partial_update_insert_on_duplicate_key VALUES ('2023-01-01', 1003, 22, 'c3', 'e3');
INSERT INTO unique_partial_update_insert_on_duplicate_key VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');

SELECT 'test basic func, stage 1';
SELECT * FROM unique_partial_update_insert_on_duplicate_key ORDER BY id;

-- data for unique key 1004: 2023-01-02, 1004, 1985320583, 'c4', 'x3'
-- data for unique key 1005: 2023-01-02, 1005, 30, 'd2', 'x3'
INSERT INTO unique_partial_update_insert_on_duplicate_key (p_date, id, number, content, extra) on duplicate key update number=toUInt32(sipHash64(content)), extra=extra VALUES ('2023-01-02', 1004, 30, 'd1', 'x3'), ('2023-01-02', 1005, 30, 'd2', 'x3');  
SELECT 'test basic func, stage 2';
SELECT * FROM unique_partial_update_insert_on_duplicate_key ORDER BY id;

-- data for unique key 1004: 2023-01-02, 1004, 162109546, 'c4', 'x3'
-- data for unique key 1005: 2023-01-02, 1005, 2759267994, 'd2', 'x3'
INSERT INTO unique_partial_update_insert_on_duplicate_key select * from unique_partial_update_insert_on_duplicate_key where p_date='2023-01-02' on duplicate key update number=toUInt32(sipHash64(content)), extra=extra;
SELECT 'test basic func, stage 3';
SELECT * FROM unique_partial_update_insert_on_duplicate_key ORDER BY id;

SELECT 'test holo expression, stage 1';
ALTER TABLE unique_partial_update_insert_on_duplicate_key drop partition '2023-01-02';
INSERT INTO unique_partial_update_insert_on_duplicate_key VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');
SELECT * FROM unique_partial_update_insert_on_duplicate_key ORDER BY id;

-- data for unique key 1004: 2023-01-02, 1004, 1985320583, 'c4', 'x3'
-- data for unique key 1005: 2023-01-02, 1005, 30, 'd2', 'x3'
INSERT INTO unique_partial_update_insert_on_duplicate_key (p_date, id, number, content, extra) on duplicate key update number=toUInt32(sipHash64(EXCLUDED.content)), extra=EXCLUDED.extra VALUES ('2023-01-02', 1004, 30, 'd1', 'x3'), ('2023-01-02', 1005, 30, 'd2', 'x3');  
SELECT 'test holo expression, stage 2';
SELECT * FROM unique_partial_update_insert_on_duplicate_key ORDER BY id;

-- data for unique key 1004: 2023-01-02, 1004, 162109546, 'c4', 'x3'
-- data for unique key 1005: 2023-01-02, 1005, 2759267994, 'd2', 'x3'
INSERT INTO unique_partial_update_insert_on_duplicate_key select * from unique_partial_update_insert_on_duplicate_key where p_date='2023-01-02' on duplicate key update number=toUInt32(sipHash64(EXCLUDED.content)), extra=EXCLUDED.extra;
SELECT 'test holo expression, stage 3';
SELECT * FROM unique_partial_update_insert_on_duplicate_key ORDER BY id;

SELECT 'test mysql expression, stage 1';
ALTER TABLE unique_partial_update_insert_on_duplicate_key drop partition '2023-01-02';
INSERT INTO unique_partial_update_insert_on_duplicate_key VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');
SELECT * FROM unique_partial_update_insert_on_duplicate_key ORDER BY id;

-- data for unique key 1004: 2023-01-02, 1004, 1985320583, 'c4', 'x3'
-- data for unique key 1005: 2023-01-02, 1005, 30, 'd2', 'x3'
INSERT INTO unique_partial_update_insert_on_duplicate_key (p_date, id, number, content, extra) on duplicate key update number=toUInt32(sipHash64(VALUES(content))), extra=VALUES(extra) VALUES ('2023-01-02', 1004, 30, 'd1', 'x3'), ('2023-01-02', 1005, 30, 'd2', 'x3');  
SELECT 'test mysql expression, stage 2';
SELECT * FROM unique_partial_update_insert_on_duplicate_key ORDER BY id;

-- data for unique key 1004: 2023-01-02, 1004, 162109546, 'c4', 'x3'
-- data for unique key 1005: 2023-01-02, 1005, 2759267994, 'd2', 'x3'
INSERT INTO unique_partial_update_insert_on_duplicate_key select * from unique_partial_update_insert_on_duplicate_key where p_date='2023-01-02' on duplicate key update number=toUInt32(sipHash64(VALUES(content))), extra=VALUES(extra);
SELECT 'test mysql expression, stage 3';
SELECT * FROM unique_partial_update_insert_on_duplicate_key ORDER BY id;

SELECT 'test update non_updatable_columns';
INSERT INTO unique_partial_update_insert_on_duplicate_key select * from unique_partial_update_insert_on_duplicate_key where p_date='2023-01-02' on duplicate key update id=toUInt32(sipHash64(content));
-- Do not take effect
-- data for unique key 1004: 2023-01-02, 1004, 162109546, 'c4', 'x3'
-- data for unique key 1005: 2023-01-02, 1005, 2759267994, 'd2', 'x3'
SELECT * FROM unique_partial_update_insert_on_duplicate_key ORDER BY id;

DROP TABLE IF EXISTS unique_partial_update_insert_on_duplicate_key;
