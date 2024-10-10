DROP TABLE IF EXISTS unique_partial_update_with_update_set;

SELECT 'test dedup in write suffix stage';
CREATE TABLE unique_partial_update_with_update_set
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
UNIQUE KEY id settings dedup_impl_version = 'dedup_in_write_suffix';

INSERT INTO unique_partial_update_with_update_set VALUES ('2023-01-01', 1001, 20, 'c1', 'e1');
INSERT INTO unique_partial_update_with_update_set VALUES ('2023-01-01', 1002, 21, 'c2', 'e2');
INSERT INTO unique_partial_update_with_update_set VALUES ('2023-01-01', 1003, 22, 'c3', 'e3');
INSERT INTO unique_partial_update_with_update_set VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');

SET insert_if_not_exists = 1;

SELECT * FROM unique_partial_update_with_update_set ORDER BY p_date, id;

-- test partial update on non existing row
SELECT 'test partial update on non existing row';
UPDATE unique_partial_update_with_update_set SET number = 30, content = 'x1' WHERE p_date = '2023-01-02' and id = 1005;
SELECT * FROM unique_partial_update_with_update_set ORDER BY p_date, id;

-- test partial update on existing row
SELECT 'test partial update on existing row';
UPDATE unique_partial_update_with_update_set SET extra = 'y1' WHERE p_date = '2023-01-02' and id = 1005;
SELECT * FROM unique_partial_update_with_update_set ORDER BY p_date, id;

-- test throw exception
UPDATE unique_partial_update_with_update_set SET extra = 'y1' WHERE p_date = '2023-01-02'; -- { serverError 36 }
UPDATE unique_partial_update_with_update_set SET extra = 'y1' WHERE id = 1005; -- { serverError 36 }
UPDATE unique_partial_update_with_update_set SET extra = 'y1' WHERE content = 'x1'; -- { serverError 36 }

DROP TABLE IF EXISTS unique_partial_update_with_update_set;

SELECT '';
SELECT 'test dedup in txn commit stage';
CREATE TABLE unique_partial_update_with_update_set
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
UNIQUE KEY id settings dedup_impl_version = 'dedup_in_txn_commit';

INSERT INTO unique_partial_update_with_update_set VALUES ('2023-01-01', 1001, 20, 'c1', 'e1');
INSERT INTO unique_partial_update_with_update_set VALUES ('2023-01-01', 1002, 21, 'c2', 'e2');
INSERT INTO unique_partial_update_with_update_set VALUES ('2023-01-01', 1003, 22, 'c3', 'e3');
INSERT INTO unique_partial_update_with_update_set VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');

SET insert_if_not_exists = 1;

SELECT * FROM unique_partial_update_with_update_set ORDER BY p_date, id;

-- test partial update on non existing row
SELECT 'test partial update on non existing row';
UPDATE unique_partial_update_with_update_set SET number = 30, content = 'x1' WHERE p_date = '2023-01-02' and id = 1005;
SELECT * FROM unique_partial_update_with_update_set ORDER BY p_date, id;

-- test partial update on existing row
SELECT 'test partial update on existing row';
UPDATE unique_partial_update_with_update_set SET extra = 'y1' WHERE p_date = '2023-01-02' and id = 1005;
SELECT * FROM unique_partial_update_with_update_set ORDER BY p_date, id;

-- test throw exception
UPDATE unique_partial_update_with_update_set SET extra = 'y1' WHERE p_date = '2023-01-02'; -- { serverError 36 }
UPDATE unique_partial_update_with_update_set SET extra = 'y1' WHERE id = 1005; -- { serverError 36 }
UPDATE unique_partial_update_with_update_set SET extra = 'y1' WHERE content = 'x1'; -- { serverError 36 }

DROP TABLE IF EXISTS unique_partial_update_with_update_set;
