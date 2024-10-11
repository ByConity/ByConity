DROP TABLE IF EXISTS unique_partial_update_query_native;

CREATE TABLE unique_partial_update_query_native
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

SYSTEM STOP DEDUP WORKER unique_partial_update_query_native;

SET enable_staging_area_for_write=0, enable_unique_partial_update=0;
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-01', 1001, 20, 'c1', 'e1');
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-01', 1002, 21, 'c2', 'e2');
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-01', 1003, 22, 'c3', 'e3');
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-02', 1005, 30, 'd2', 'x2');

SELECT 'Non staging area test, stage 1';
SELECT * FROM unique_partial_update_query_native ORDER BY id;

SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
-- data for unique key 1005: 2023-01-02, 1005, 30, d2, x3
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, 'd1', 'x3', 'extra');  
-- data for unique key 1005: 2023-01-02, 1005, 30, d3, x3
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, 'd3', 'x2', 'content');  

SELECT 'Non staging area test, stage 2';
SELECT * FROM unique_partial_update_query_native ORDER BY id;

SELECT 'Staging area test, normal + partial, stage 1';
ALTER TABLE unique_partial_update_query_native DROP PARTITION id '20230102';

SET enable_staging_area_for_write=0, enable_unique_partial_update=0;
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-02', 1005, 30, 'd2', 'x2');

SELECT * FROM unique_partial_update_query_native ORDER BY id;

SET enable_staging_area_for_write=1, enable_unique_partial_update=0;
-- data for unique key 1005: 2023-01-02, 1005, 40, d1, x1
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 40, 'd1', 'x1', 'number');  

SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
-- data for unique key 1005: 2023-01-02, 1005, 20, d1, x1
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 20, 'd2', 'x2', 'number'); 

SELECT 'Staging area test, normal + partial, stage 2';
SELECT * FROM unique_partial_update_query_native ORDER BY id;

SELECT 'Staging area test, partial + normal, stage 1';
ALTER TABLE unique_partial_update_query_native DROP PARTITION id '20230102';

SET enable_staging_area_for_write=0, enable_unique_partial_update=0;
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-02', 1005, 30, 'd2', 'x2');

SELECT * FROM unique_partial_update_query_native ORDER BY id;

SET enable_staging_area_for_write=1, enable_unique_partial_update=1;
-- data for unique key 1005: 2023-01-02, 1005, 30, d2, x3
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, 'd1', 'x3', 'extra');  

SET enable_staging_area_for_write=0, enable_unique_partial_update=0;
-- data for unique key 1005: 2023-01-02, 1005, 40, d1, x1
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 40, 'd1', 'x1', 'number');  

SELECT 'Staging area test, partial + normal, stage 2';
SELECT * FROM unique_partial_update_query_native ORDER BY id;

SELECT 'Staging area + alter test, stage 1';
ALTER TABLE unique_partial_update_query_native DROP PARTITION id '20230102';

SET enable_staging_area_for_write=0, enable_unique_partial_update=0;
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-02', 1005, 30, 'd2', 'x2');

SELECT * FROM unique_partial_update_query_native ORDER BY id;

SET enable_staging_area_for_write=1, enable_unique_partial_update=1;
-- data for unique key 1005: 2023-01-02, 1005, 30, d2, x3
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, 'd1', 'x3', 'extra');  

ALTER TABLE unique_partial_update_query_native ADD COLUMN test_col String AFTER content;
SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
-- data for unique key 1005: 2023-01-02, 1005, 30, d2, test, x3
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, test_col, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, 'd3', 'test', 'x2', 'test_col');  

SELECT 'Staging area + alter test, stage 2';
SELECT * FROM unique_partial_update_query_native ORDER BY id;

SELECT 'Modify partial_update_optimize_for_batch_task test, stage 1';
ALTER TABLE unique_partial_update_query_native DROP COLUMN test_col;
ALTER TABLE unique_partial_update_query_native DROP PARTITION id '20230102';

SELECT * FROM unique_partial_update_query_native ORDER BY id;

SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
-- data for unique key 1004: 2023-01-02, 1004, 0, '', e4
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1004, 23, 'c4', 'e4', 'extra');  

ALTER TABLE unique_partial_update_query_native modify setting partial_update_optimize_for_batch_task = 0;
-- data for unique key 1005: 2023-01-02, 1005, 30, d3, x2
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, 'd3', 'x2', 'extra');  

SELECT 'Modify partial_update_optimize_for_batch_task test, stage 2';
SELECT * FROM unique_partial_update_query_native ORDER BY id;

SELECT 'Modify partial_update_query_columns_thread_size test, stage 1';
ALTER TABLE unique_partial_update_query_native DROP PARTITION id '20230102';
ALTER TABLE unique_partial_update_query_native modify setting partial_update_optimize_for_batch_task = 1;

SET enable_staging_area_for_write=0, enable_unique_partial_update=0;
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');
INSERT INTO unique_partial_update_query_native VALUES ('2023-01-02', 1005, 30, 'd2', 'x2');

SELECT * FROM unique_partial_update_query_native ORDER BY id;

SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
-- data for unique key 1004: 2023-01-02, 1005, 30, d2, x3
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, 'd1', 'x3', 'extra'); 

ALTER TABLE unique_partial_update_query_native modify setting partial_update_query_columns_thread_size = 8;
-- data for unique key 1005: 2023-01-02, 1005, 30, d3, x3
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, 'd3', 'x2', 'content');  

SELECT 'Modify partial_update_optimize_for_batch_task test, stage 2';
SELECT * FROM unique_partial_update_query_native ORDER BY id;

SELECT 'Specify _update_columns_ corner case test, stage 1';
ALTER TABLE unique_partial_update_query_native DROP PARTITION id '20230102';

SELECT * FROM unique_partial_update_query_native ORDER BY id;

SET enable_staging_area_for_write=0, enable_unique_partial_update=1;
-- data for unique key 1004: 2023-01-02, 1004, 23, c4, e4
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1004, 23, 'c4', 'e4', ''); 
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1004, 24, 'c5', 'e5', 'p_date,id'); 

-- data for unique key 1005: 2023-01-02, 1005, 30, d3, x2
INSERT INTO unique_partial_update_query_native (p_date, id, number, content, extra, _update_columns_) VALUES ('2023-01-02', 1005, 30, 'd3', 'x2', 'p_date,id,number,content,extra');  

SELECT 'Specify _update_columns_ corner case test, stage 2';
SELECT * FROM unique_partial_update_query_native ORDER BY id;

DROP TABLE IF EXISTS unique_partial_update_query_native;
