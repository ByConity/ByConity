DROP TABLE IF EXISTS unique_partial_update_query_nullable;

CREATE TABLE unique_partial_update_query_nullable
(
    `p_date` Date,
    `id` UInt32,
    `number` Nullable(UInt32),
    `content` Nullable(String),
    `extra` String REPLACE_IF_NOT_NULL
)
ENGINE = CnchMergeTree()
PARTITION BY p_date
ORDER BY id
UNIQUE KEY id
SETTINGS enable_unique_partial_update = 1; -- { serverError 49 }

CREATE TABLE unique_partial_update_query_nullable
(
    `p_date` Date,
    `id` UInt32,
    `number` Nullable(UInt32),
    `content` Nullable(String),
    `extra` String
)
ENGINE = CnchMergeTree()
PARTITION BY p_date
ORDER BY id
UNIQUE KEY id
SETTINGS enable_unique_partial_update = 1;

SYSTEM STOP DEDUP WORKER unique_partial_update_query_nullable;

SET enable_staging_area_for_write=0, enable_unique_partial_update=0;
INSERT INTO unique_partial_update_query_nullable VALUES ('2023-01-01', 1001, 20, 'c1', 'e1');
INSERT INTO unique_partial_update_query_nullable VALUES ('2023-01-01', 1002, 21, 'c2', 'e2');
INSERT INTO unique_partial_update_query_nullable VALUES ('2023-01-01', 1003, 22, 'c3', 'e3');
INSERT INTO unique_partial_update_query_nullable VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');
INSERT INTO unique_partial_update_query_nullable VALUES ('2023-01-02', 1005, 30, 'd2', 'x2');

SELECT 'test enable replace_if_not_null with non-nullable column, stage 1';
SELECT * FROM unique_partial_update_query_nullable ORDER BY id;

SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
-- data for unique key 1005: 2023-01-02, 1005, 30, d2, ''
INSERT INTO unique_partial_update_query_nullable (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, 'd1', null, 'extra');  

ALTER TABLE unique_partial_update_query_nullable modify column content REPLACE_IF_NOT_NULL;
-- data for unique key 1005: 2023-01-02, 1005, 30, d2, ''
INSERT INTO unique_partial_update_query_nullable (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, null, 'x2', 'content');  

SELECT 'test enable replace_if_not_null with non-nullable column, stage 2';
SELECT * FROM unique_partial_update_query_nullable ORDER BY id;

ALTER TABLE unique_partial_update_query_nullable modify column extra REPLACE_IF_NOT_NULL; -- { serverError 49 }
ALTER TABLE unique_partial_update_query_nullable modify column content remove REPLACE_IF_NOT_NULL;
-- data for unique key 1005: 2023-01-02, 1005, 30, NULL, ''
INSERT INTO unique_partial_update_query_nullable (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, null, 'x2', 'content');  
SELECT 'test enable replace_if_not_null with non-nullable column, stage 3';
SELECT * FROM unique_partial_update_query_nullable ORDER BY id;

ALTER TABLE unique_partial_update_query_nullable modify setting partial_update_replace_if_not_null = 1;

SELECT 'test enable replace_if_not_null with non-nullable column, stage 4';
-- data for unique key 1005: 2023-01-02, 1005, 30, NULL, ''
INSERT INTO unique_partial_update_query_nullable (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, null, null, 'x2', 'number');  
SELECT * FROM unique_partial_update_query_nullable ORDER BY id;

DROP TABLE IF EXISTS unique_partial_update_query_nullable;

CREATE TABLE unique_partial_update_query_nullable 
(
    id Int32, 
    a Nullable(Int32), 
    b LowCardinality(String), 
    c Nullable(FixedString(10)), 
    d Nullable(Array(Int32)), 
    e Map(String, Int32)) 
Engine=CnchMergeTree() 
ORDER BY id 
UNIQUE KEY id 
SETTINGS enable_unique_partial_update = 1, partial_update_enable_merge_map = 1, partial_update_replace_if_not_null = 1;

SELECT 'test enable replace_if_not_null with non-nullable column, stage 5';
SET enable_staging_area_for_write=0, enable_unique_partial_update = 0;
INSERT INTO unique_partial_update_query_nullable VALUES
(1, 0, 'x', 'x', [1, 2, 3], {'a': 1}), (1, 0, 'z', 'z', [1, 2], {'b': 3, 'c': 4}), (1, 2, 'm', 'm', [], {'a': 2, 'd': 7}), (2, 0, 'q', 'q', [], {'a': 1}), (2, 0, 't', 't', [4, 5, 6], {'e': 8}), (2, 3, '',  '',  [5, 6], {'e': 6, 'f': 9});
SELECT * FROM unique_partial_update_query_nullable ORDER BY id;

SET enable_staging_area_for_write=0, enable_unique_partial_update = 1;
SELECT 'test enable replace_if_not_null with non-nullable column, stage 6';
INSERT INTO unique_partial_update_query_nullable VALUES (1, null, null, null, null, null);
SELECT * FROM unique_partial_update_query_nullable ORDER BY id;

DROP TABLE IF EXISTS unique_partial_update_query_nullable;
