DROP TABLE IF EXISTS u10117_uniquekey_test_bucket_commands;
DROP TABLE IF EXISTS u10117_uniquekey_test_bucket_commands_helper;

CREATE TABLE u10117_uniquekey_test_bucket_commands (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY sipHash64(id) INTO 4 BUCKETS ORDER BY s;
SYSTEM DEDUP u10117_uniquekey_test_bucket_commands FOR REPAIR;
SYSTEM DEDUP u10117_uniquekey_test_bucket_commands BUCKET 0 FOR REPAIR;  -- { serverError 36 }

SELECT 'Ensure bucket number is assigned to a part in bucket table';
INSERT INTO u10117_uniquekey_test_bucket_commands select '2024-03-26', number, 'value' from system.numbers limit 10;
INSERT INTO u10117_uniquekey_test_bucket_commands select '2024-03-26', number, 'value' from system.numbers limit 10;
INSERT INTO u10117_uniquekey_test_bucket_commands select '2024-03-26', number, 'value' from system.numbers limit 10;
SELECT _bucket_number, * FROM u10117_uniquekey_test_bucket_commands ORDER BY id;
SELECT count() FROM u10117_uniquekey_test_bucket_commands WHERE _bucket_number = 0;
ALTER TABLE u10117_uniquekey_test_bucket_commands DETACH PARTITION ID '20240326' BUCKET 0;
SELECT 'detach partition id 20240326 bucket 0';
SELECT _bucket_number, * FROM u10117_uniquekey_test_bucket_commands ORDER BY id;
ALTER TABLE u10117_uniquekey_test_bucket_commands DETACH PARTITION ID '20240326';
SELECT 'detach partition id 20240326';
SELECT _bucket_number, * FROM u10117_uniquekey_test_bucket_commands ORDER BY id;

ALTER TABLE u10117_uniquekey_test_bucket_commands ATTACH PARTITION ID '20240326' BUCKET 1;
SELECT 'attach partition id 20240326 bucket 1';
SELECT _bucket_number, * FROM u10117_uniquekey_test_bucket_commands ORDER BY id;
ALTER TABLE u10117_uniquekey_test_bucket_commands ATTACH PARTITION ID '20240326';
SELECT 'attach partition id 20240326';
SELECT _bucket_number, * FROM u10117_uniquekey_test_bucket_commands ORDER BY id;

SELECT '';
SELECT 'construct duplicate key case, test repair commands';
CREATE TABLE u10117_uniquekey_test_bucket_commands_helper (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY sipHash64(id) INTO 4 BUCKETS ORDER BY s;
INSERT INTO u10117_uniquekey_test_bucket_commands_helper select '2024-03-26', number, 'value' from system.numbers limit 10;
INSERT INTO u10117_uniquekey_test_bucket_commands_helper select '2024-03-26', number, 'value' from system.numbers limit 10;
INSERT INTO u10117_uniquekey_test_bucket_commands_helper select '2024-03-26', number, 'value' from system.numbers limit 10;
ALTER TABLE u10117_uniquekey_test_bucket_commands_helper DETACH PARTITION ID '20240326';
SET enable_unique_table_attach_without_dedup = 1;
ALTER TABLE u10117_uniquekey_test_bucket_commands ATTACH DETACHED PARTITION ID '20240326' FROM u10117_uniquekey_test_bucket_commands_helper;
SELECT id, _bucket_number FROM u10117_uniquekey_test_bucket_commands GROUP BY id, _bucket_number HAVING count() > 1 ORDER BY id;
SYSTEM DEDUP u10117_uniquekey_test_bucket_commands PARTITION ID '20240326' BUCKET 2 FOR REPAIR;
SELECT id, _bucket_number FROM u10117_uniquekey_test_bucket_commands GROUP BY id, _bucket_number HAVING count() > 1 ORDER BY id;
SYSTEM DEDUP u10117_uniquekey_test_bucket_commands PARTITION ID '20240326' BUCKET 1 FOR REPAIR;
SELECT id, _bucket_number FROM u10117_uniquekey_test_bucket_commands GROUP BY id, _bucket_number HAVING count() > 1 ORDER BY id;
SYSTEM DEDUP u10117_uniquekey_test_bucket_commands PARTITION ID '20240326' FOR REPAIR;
SELECT id, _bucket_number FROM u10117_uniquekey_test_bucket_commands GROUP BY id, _bucket_number HAVING count() > 1 ORDER BY id;

SELECT '';
SELECT 'modify cluster by, test expected_table_definition_hash setting';
ALTER TABLE u10117_uniquekey_test_bucket_commands MODIFY CLUSTER BY sipHash64(id) INTO 2 BUCKETS;
INSERT INTO u10117_uniquekey_test_bucket_commands select '2024-03-26', number, 'value' from system.numbers limit 10, 10;
INSERT INTO u10117_uniquekey_test_bucket_commands select '2024-03-26', number, 'value' from system.numbers limit 10, 10;
INSERT INTO u10117_uniquekey_test_bucket_commands select '2024-03-26', number, 'value' from system.numbers limit 10, 10;
SELECT _bucket_number, * FROM u10117_uniquekey_test_bucket_commands ORDER BY id;
SET expected_table_definition_hash = 17680458339604861779;
ALTER TABLE u10117_uniquekey_test_bucket_commands DETACH PARTITION ID '20240326' BUCKET 0;
SELECT _bucket_number, * FROM u10117_uniquekey_test_bucket_commands ORDER BY id;
SET expected_table_definition_hash = 17398319369202094954;
ALTER TABLE u10117_uniquekey_test_bucket_commands ATTACH PARTITION ID '20240326' BUCKET 0;
SELECT _bucket_number, * FROM u10117_uniquekey_test_bucket_commands ORDER BY id;
SET expected_table_definition_hash = 0;
ALTER TABLE u10117_uniquekey_test_bucket_commands ATTACH PARTITION ID '20240326' BUCKET 0;
SELECT _bucket_number, * FROM u10117_uniquekey_test_bucket_commands ORDER BY id;

DROP TABLE IF EXISTS u10117_uniquekey_test_bucket_commands;
DROP TABLE IF EXISTS u10117_uniquekey_test_bucket_commands_helper;
