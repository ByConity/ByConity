DROP TABLE IF EXISTS unique_update_query;

SELECT 'test dedup in write suffix stage';
CREATE TABLE unique_update_query
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
UNIQUE KEY id settings dedup_impl_version = 'dedup_in_write_suffix';

INSERT INTO unique_update_query VALUES ('2023-01-01', 1001, 9, 'c1', 'e1');
INSERT INTO unique_update_query VALUES ('2023-01-01', 1002, 99, 'c2', 'e2');
INSERT INTO unique_update_query VALUES ('2023-01-01', 1003, 999, 'c3', 'e3');
INSERT INTO unique_update_query VALUES ('2023-01-02', 1004, 9999, 'c4', 'e4');

SELECT * FROM unique_update_query ORDER BY id;
-- test update columns with filter
UPDATE unique_update_query SET number = 10, content = 'x1' WHERE p_date = '2023-01-02';
SELECT '';
SELECT * FROM unique_update_query ORDER BY id;
-- test update specific rows by using filter and order by limit
UPDATE unique_update_query SET number = number+1, content = concat('new_',content) WHERE p_date = '2023-01-01' ORDER BY id LIMIT 1;
SELECT '';
SELECT * FROM unique_update_query ORDER BY id;
-- test swap two columns
UPDATE unique_update_query SET content=extra, extra=content WHERE p_date = '2023-01-01';
SELECT '';
SELECT * FROM unique_update_query ORDER BY id;
-- test update unique keys (throw exception)
UPDATE unique_update_query SET id=1000 WHERE p_date = '2023-01-01'; -- { serverError 36 }

DROP TABLE IF EXISTS unique_update_query;

SELECT '';
SELECT 'test dedup in txn commit stage';
CREATE TABLE unique_update_query
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
UNIQUE KEY id settings dedup_impl_version = 'dedup_in_txn_commit';

INSERT INTO unique_update_query VALUES ('2023-01-01', 1001, 9, 'c1', 'e1');
INSERT INTO unique_update_query VALUES ('2023-01-01', 1002, 99, 'c2', 'e2');
INSERT INTO unique_update_query VALUES ('2023-01-01', 1003, 999, 'c3', 'e3');
INSERT INTO unique_update_query VALUES ('2023-01-02', 1004, 9999, 'c4', 'e4');

SELECT * FROM unique_update_query ORDER BY id;
-- test update columns with filter
UPDATE unique_update_query SET number = 10, content = 'x1' WHERE p_date = '2023-01-02';
SELECT '';
SELECT * FROM unique_update_query ORDER BY id;
-- test update specific rows by using filter and order by limit
UPDATE unique_update_query SET number = number+1, content = concat('new_',content) WHERE p_date = '2023-01-01' ORDER BY id LIMIT 1;
SELECT '';
SELECT * FROM unique_update_query ORDER BY id;
-- test swap two columns
UPDATE unique_update_query SET content=extra, extra=content WHERE p_date = '2023-01-01';
SELECT '';
SELECT * FROM unique_update_query ORDER BY id;
-- test update unique keys (throw exception)
UPDATE unique_update_query SET id=1000 WHERE p_date = '2023-01-01'; -- { serverError 36 }

DROP TABLE IF EXISTS unique_update_query;
