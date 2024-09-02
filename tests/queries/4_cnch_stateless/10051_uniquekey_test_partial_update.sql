DROP TABLE IF EXISTS unique_partial_update_query;

CREATE TABLE unique_partial_update_query
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
UNIQUE KEY id;

INSERT INTO unique_partial_update_query VALUES ('2023-01-01', 1001, 20, 'c1', 'e1');
INSERT INTO unique_partial_update_query VALUES ('2023-01-01', 1002, 21, 'c2', 'e2');
INSERT INTO unique_partial_update_query VALUES ('2023-01-01', 1003, 22, 'c3', 'e3');
INSERT INTO unique_partial_update_query VALUES ('2023-01-02', 1004, 23, 'c4', 'e4');

SET insert_if_not_exists = 1;

SELECT * FROM unique_partial_update_query ORDER BY p_date, id;

-- test partial update on non existing row
SELECT 'test partial update on non existing row';
UPDATE unique_partial_update_query SET number = 30, content = 'x1' WHERE p_date = '2023-01-02' and id = 1005;
SELECT * FROM unique_partial_update_query ORDER BY p_date, id;

-- test partial update on existing row
SELECT 'test partial update on existing row';
UPDATE unique_partial_update_query SET extra = 'y1' WHERE p_date = '2023-01-02' and id = 1005;
SELECT * FROM unique_partial_update_query ORDER BY p_date, id;

-- test throw exception
UPDATE unique_partial_update_query SET extra = 'y1' WHERE p_date = '2023-01-02'; -- { serverError 36 }
UPDATE unique_partial_update_query SET extra = 'y1' WHERE id = 1005; -- { serverError 36 }
UPDATE unique_partial_update_query SET extra = 'y1' WHERE content = 'x1'; -- { serverError 36 }

DROP TABLE IF EXISTS unique_partial_update_query;
