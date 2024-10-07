DROP TABLE IF EXISTS unique_with_exception_on_unique_key_duplication;

SELECT 'test dedup in write suffix stage';
CREATE TABLE unique_with_exception_on_unique_key_duplication(
    `event_time` DateTime,
    `product_id` UInt64,
    `city` String,
    `category` String,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = CnchMergeTree()
PARTITION BY toDate(event_time)
ORDER BY (event_time, product_id)
UNIQUE KEY product_id settings dedup_impl_version = 'dedup_in_write_suffix';

-- Rely on sync deduplication
SYSTEM STOP DEDUP WORKER unique_with_exception_on_unique_key_duplication;

INSERT INTO unique_with_exception_on_unique_key_duplication VALUES ('2020-10-29 23:40:00', 10001, 'Beijing', '男装', 5, 500), ('2020-10-29 23:40:00', 10002, 'Beijing', '男装', 2, 200);

INSERT INTO unique_with_exception_on_unique_key_duplication Format Values SETTINGS enable_staging_area_for_write = 1, dedup_key_mode = 'throw'
('2020-10-29 23:40:00', 10001, 'Beijing', '男装', 4, 500); -- { serverError 36 }

INSERT INTO unique_with_exception_on_unique_key_duplication Format Values SETTINGS enable_staging_area_for_write = 1
('2020-10-29 23:40:00', 10003, 'Beijing', '男装', 1, 100);

-- Pay special attention to this case, insert with dedup_key_mode = 'throw' does not throw exception for invisible data(in the staging area)
INSERT INTO unique_with_exception_on_unique_key_duplication Format Values SETTINGS dedup_key_mode = 'throw'
('2020-10-29 23:50:00', 10003, 'Beijing', '男装', 2, 200); 

INSERT INTO unique_with_exception_on_unique_key_duplication VALUES ('2020-10-29 23:50:00', 10004, 'Beijing', '男装', 2, 200);

INSERT INTO unique_with_exception_on_unique_key_duplication Format Values SETTINGS dedup_key_mode = 'throw'
('2020-10-29 23:50:00', 10004, 'Beijing', '男装', 3, 300), ('2020-10-29 23:50:00', 10005, 'Beijing', '男装', 2, 200); -- { serverError 117 }

-- block contains duplicate unique keys
INSERT INTO unique_with_exception_on_unique_key_duplication Format Values SETTINGS dedup_key_mode = 'throw'
('2020-10-29 23:50:00', 10006, 'Beijing', '男装', 6, 600), ('2020-10-29 23:50:00', 10006, 'Beijing', '男装', 6, 600); -- { serverError 117 }

SELECT * FROM unique_with_exception_on_unique_key_duplication order by event_time, product_id;

DROP TABLE IF EXISTS unique_with_exception_on_unique_key_duplication;

SELECT '';
SELECT 'test dedup in txn commit stage';
CREATE TABLE unique_with_exception_on_unique_key_duplication(
    `event_time` DateTime,
    `product_id` UInt64,
    `city` String,
    `category` String,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = CnchMergeTree()
PARTITION BY toDate(event_time)
ORDER BY (event_time, product_id)
UNIQUE KEY product_id settings dedup_impl_version = 'dedup_in_txn_commit';

-- Rely on sync deduplication
SYSTEM STOP DEDUP WORKER unique_with_exception_on_unique_key_duplication;

INSERT INTO unique_with_exception_on_unique_key_duplication VALUES ('2020-10-29 23:40:00', 10001, 'Beijing', '男装', 5, 500), ('2020-10-29 23:40:00', 10002, 'Beijing', '男装', 2, 200);

INSERT INTO unique_with_exception_on_unique_key_duplication Format Values SETTINGS enable_staging_area_for_write = 1, dedup_key_mode = 'throw'
('2020-10-29 23:40:00', 10001, 'Beijing', '男装', 4, 500); -- { serverError 36 }

INSERT INTO unique_with_exception_on_unique_key_duplication Format Values SETTINGS enable_staging_area_for_write = 1
('2020-10-29 23:40:00', 10003, 'Beijing', '男装', 1, 100);

-- Pay special attention to this case, insert with dedup_key_mode = 'throw' does not throw exception for invisible data(in the staging area)
INSERT INTO unique_with_exception_on_unique_key_duplication Format Values SETTINGS dedup_key_mode = 'throw'
('2020-10-29 23:50:00', 10003, 'Beijing', '男装', 2, 200); 

INSERT INTO unique_with_exception_on_unique_key_duplication VALUES ('2020-10-29 23:50:00', 10004, 'Beijing', '男装', 2, 200);

INSERT INTO unique_with_exception_on_unique_key_duplication Format Values SETTINGS dedup_key_mode = 'throw'
('2020-10-29 23:50:00', 10004, 'Beijing', '男装', 3, 300), ('2020-10-29 23:50:00', 10005, 'Beijing', '男装', 2, 200); -- { serverError 117 }

-- block contains duplicate unique keys
INSERT INTO unique_with_exception_on_unique_key_duplication Format Values SETTINGS dedup_key_mode = 'throw'
('2020-10-29 23:50:00', 10006, 'Beijing', '男装', 6, 600), ('2020-10-29 23:50:00', 10006, 'Beijing', '男装', 6, 600); -- { serverError 117 }

SELECT * FROM unique_with_exception_on_unique_key_duplication order by event_time, product_id;

DROP TABLE IF EXISTS unique_with_exception_on_unique_key_duplication;
