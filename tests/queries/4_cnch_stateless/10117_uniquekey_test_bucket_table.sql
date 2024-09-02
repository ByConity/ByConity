DROP TABLE IF EXISTS u10117_uniquekey_test_bucket;
DROP TABLE IF EXISTS u10117_uniquekey_test_bucket2;
DROP TABLE IF EXISTS u10117_uniquekey_test_normal;

select 'test partition level unique key and cluster by is same with unique key';
CREATE TABLE u10117_uniquekey_test_bucket (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY sipHash64(id) INTO 1 BUCKETS ORDER BY s;
CREATE TABLE u10117_uniquekey_test_bucket2 (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY sipHash64(id) INTO 1 BUCKETS ORDER BY s;
CREATE TABLE u10117_uniquekey_test_normal (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id ORDER BY s;

SELECT 'Ensure bucket number is assigned to a part in bucket table';
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-06-26', 3, '3a');
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-06-26', 3, '3a'), ('2023-06-26', 1, '1b'), ('2023-06-26', 1, '1a'), ('2023-06-26', 1, '1c'), ('2023-06-26', 3, '3b');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket' and active;

SELECT 'Ensure join queries between bucket tables work correctly';
INSERT INTO u10117_uniquekey_test_bucket2 VALUES ('2023-06-26', 0, '0a'), ('2023-06-26', 1, '1d'), ('2023-06-26', 1, '1e'), ('2023-06-26', 4, '4a'), ('2023-06-26', 1, '1b');
SELECT * FROM u10117_uniquekey_test_bucket2 ORDER BY s;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket2' and active;
SELECT b1.s, id, b2.s FROM u10117_uniquekey_test_bucket b1 JOIN u10117_uniquekey_test_bucket2 b2 USING (id);

SELECT 'ALTER MODIFY CLUSTER KEY DEFINITION';
ALTER TABLE u10117_uniquekey_test_bucket MODIFY CLUSTER BY sipHash64(id) INTO 3 BUCKETS;
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 0, '0a'), ('2023-06-26', 1, '1d'), ('2023-06-26', 1, '1e'), ('2023-06-26', 4, '4a'), ('2023-06-26', 1, '1b');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s;
SELECT bucket_number, rows_count, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket' and active order by bucket_number;

SELECT 'AFTER MODIFY CLUSTER KEY, test insert one row';
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-25', 0, '0a'), ('2023-06-25', 1, '1d'), ('2023-06-25', 1, '1e'), ('2023-06-25', 4, '4a'), ('2023-06-25', 1, '1b');
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-25', 0, '00a');
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-25', 1, '11d');
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-25', 4, '44a');
SELECT * FROM u10117_uniquekey_test_bucket WHERE d = '2023-06-25' order by id;

SELECT 'DROP bucket table definition, INSERT, ensure bucket number of new part is -1, ban recluster commands';
ALTER TABLE u10117_uniquekey_test_bucket2 DROP CLUSTER;
INSERT INTO u10117_uniquekey_test_bucket2 VALUES ('2023-06-25', 0, '0a');
SELECT * FROM u10117_uniquekey_test_bucket2 ORDER BY s, d;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket2' and active order by partition, bucket_number;
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION '2023-06-26';  -- { serverError 344 }
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION ID '20230626'; -- { serverError 344 }
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION WHERE id > 0;  -- { serverError 344 }

DROP TABLE IF EXISTS u10117_uniquekey_test_bucket;
DROP TABLE IF EXISTS u10117_uniquekey_test_bucket2;
DROP TABLE IF EXISTS u10117_uniquekey_test_normal;

SELECT '';
select 'test partition level unique key and cluster by is different with unique key';
CREATE TABLE u10117_uniquekey_test_bucket (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY s INTO 1 BUCKETS ORDER BY s;
CREATE TABLE u10117_uniquekey_test_bucket2 (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY s INTO 1 BUCKETS ORDER BY s;
CREATE TABLE u10117_uniquekey_test_normal (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id ORDER BY s;

SELECT 'Ensure bucket number is assigned to a part in bucket table';
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-06-26', 3, '3a');
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-06-26', 3, '3a'), ('2023-06-26', 1, '1b'), ('2023-06-26', 1, '1a'), ('2023-06-26', 1, '1c'), ('2023-06-26', 3, '3b');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket' and active;

SELECT 'Ensure join queries between bucket tables work correctly';
INSERT INTO u10117_uniquekey_test_bucket2 VALUES ('2023-06-26', 0, '0a'), ('2023-06-26', 1, '1d'), ('2023-06-26', 1, '1e'), ('2023-06-26', 4, '4a'), ('2023-06-26', 1, '1b');
SELECT * FROM u10117_uniquekey_test_bucket2 ORDER BY s;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket2' and active;
SELECT b1.s, id, b2.s FROM u10117_uniquekey_test_bucket b1 JOIN u10117_uniquekey_test_bucket2 b2 USING (id);

SELECT 'ALTER MODIFY CLUSTER KEY DEFINITION';
ALTER TABLE u10117_uniquekey_test_bucket MODIFY CLUSTER BY s INTO 3 BUCKETS;
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 0, '0a'), ('2023-06-26', 1, '1d'), ('2023-06-26', 1, '1e'), ('2023-06-26', 4, '4a'), ('2023-06-26', 1, '1b');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s;
SELECT bucket_number, rows_count, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket' and active order by bucket_number;

SELECT 'DROP bucket table definition, INSERT, ensure bucket number of new part is -1, ban recluster commands';
ALTER TABLE u10117_uniquekey_test_bucket2 DROP CLUSTER;
INSERT INTO u10117_uniquekey_test_bucket2 VALUES ('2023-06-25', 0, '0a');
SELECT * FROM u10117_uniquekey_test_bucket2 ORDER BY s, d;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket2' and active order by partition, bucket_number;
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION '2023-06-26';  -- { serverError 344 }
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION ID '20230626'; -- { serverError 344 }
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION WHERE id > 0;  -- { serverError 344 }

SELECT 'Test there has duplicated keys in block';
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-24', 1, '1a'), ('2023-06-24', 2, '2a'), ('2023-06-24', 3, '3a'), ('2023-06-24', 1, '1b'), ('2023-06-24', 2, '2b'), ('2023-06-24', 3, '3b');
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-25', 1, '1b'), ('2023-06-25', 2, '2b'), ('2023-06-25', 3, '3b'), ('2023-06-25', 1, '1a'), ('2023-06-25', 2, '2a'), ('2023-06-25', 3, '3a');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY d, id;

DROP TABLE IF EXISTS u10117_uniquekey_test_bucket;
DROP TABLE IF EXISTS u10117_uniquekey_test_bucket2;
DROP TABLE IF EXISTS u10117_uniquekey_test_normal;

SELECT '';

select 'test table level unique key and cluster by is same with unique key';
CREATE TABLE u10117_uniquekey_test_bucket (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY sipHash64(id) INTO 1 BUCKETS ORDER BY s SETTINGS partition_level_unique_keys = 0;
CREATE TABLE u10117_uniquekey_test_bucket2 (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY sipHash64(id) INTO 1 BUCKETS ORDER BY s SETTINGS partition_level_unique_keys = 0;
CREATE TABLE u10117_uniquekey_test_normal (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id ORDER BY s SETTINGS partition_level_unique_keys = 0;

SELECT 'Ensure bucket number is assigned to a part in bucket table';
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-06-26', 3, '3a');
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-06-26', 3, '3a'), ('2023-06-26', 1, '1b'), ('2023-06-26', 1, '1a'), ('2023-06-26', 1, '1c'), ('2023-06-26', 3, '3b');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket' and active;

SELECT 'Ensure join queries between bucket tables work correctly';
INSERT INTO u10117_uniquekey_test_bucket2 VALUES ('2023-06-26', 0, '0a'), ('2023-06-26', 1, '1d'), ('2023-06-26', 1, '1e'), ('2023-06-26', 4, '4a'), ('2023-06-26', 1, '1b');
SELECT * FROM u10117_uniquekey_test_bucket2 ORDER BY s;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket2' and active;
SELECT b1.s, id, b2.s FROM u10117_uniquekey_test_bucket b1 JOIN u10117_uniquekey_test_bucket2 b2 USING (id);

SELECT 'ALTER MODIFY CLUSTER KEY DEFINITION';
ALTER TABLE u10117_uniquekey_test_bucket MODIFY CLUSTER BY sipHash64(id) INTO 3 BUCKETS;
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 0, '0a'), ('2023-06-26', 1, '1d'), ('2023-06-26', 1, '1e'), ('2023-06-26', 4, '4a'), ('2023-06-26', 1, '1b');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s;
SELECT bucket_number, rows_count, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket' and active order by bucket_number;

SELECT 'AFTER MODIFY CLUSTER KEY, test insert one row';
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-25', 0, '00a');
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-25', 1, '11d');
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-25', 4, '44a');
SELECT * FROM u10117_uniquekey_test_bucket order by id;

SELECT 'DROP bucket table definition, INSERT, ensure bucket number of new part is -1, ban recluster commands';
ALTER TABLE u10117_uniquekey_test_bucket2 DROP CLUSTER;
INSERT INTO u10117_uniquekey_test_bucket2 VALUES ('2023-06-25', 0, '0a');
SELECT * FROM u10117_uniquekey_test_bucket2 ORDER BY s, d;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket2' and active order by partition, bucket_number;
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION '2023-06-26';  -- { serverError 344 }
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION ID '20230626'; -- { serverError 344 }
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION WHERE id > 0;  -- { serverError 344 }

DROP TABLE IF EXISTS u10117_uniquekey_test_bucket;
DROP TABLE IF EXISTS u10117_uniquekey_test_bucket2;
DROP TABLE IF EXISTS u10117_uniquekey_test_normal;

SELECT '';
select 'test table level unique key and cluster by is different with unique key';
CREATE TABLE u10117_uniquekey_test_bucket (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY s INTO 1 BUCKETS ORDER BY s SETTINGS partition_level_unique_keys = 0;
CREATE TABLE u10117_uniquekey_test_bucket2 (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY s INTO 1 BUCKETS ORDER BY s SETTINGS partition_level_unique_keys = 0;
CREATE TABLE u10117_uniquekey_test_normal (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id ORDER BY s SETTINGS partition_level_unique_keys = 0;

SELECT 'Ensure bucket number is assigned to a part in bucket table';
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-06-26', 3, '3a');
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-06-26', 3, '3a'), ('2023-06-26', 1, '1b'), ('2023-06-26', 1, '1a'), ('2023-06-26', 1, '1c'), ('2023-06-26', 3, '3b');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket' and active;

SELECT 'Ensure join queries between bucket tables work correctly';
INSERT INTO u10117_uniquekey_test_bucket2 VALUES ('2023-06-26', 0, '0a'), ('2023-06-26', 1, '1d'), ('2023-06-26', 1, '1e'), ('2023-06-26', 4, '4a'), ('2023-06-26', 1, '1b');
SELECT * FROM u10117_uniquekey_test_bucket2 ORDER BY s;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket2' and active;
SELECT b1.s, id, b2.s FROM u10117_uniquekey_test_bucket b1 JOIN u10117_uniquekey_test_bucket2 b2 USING (id);

SELECT 'ALTER MODIFY CLUSTER KEY DEFINITION';
ALTER TABLE u10117_uniquekey_test_bucket MODIFY CLUSTER BY s INTO 3 BUCKETS;
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 0, '0a'), ('2023-06-26', 1, '1d'), ('2023-06-26', 1, '1e'), ('2023-06-26', 4, '4a'), ('2023-06-26', 1, '1b');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s;
SELECT bucket_number, rows_count, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket' and active order by bucket_number;

SELECT 'DROP bucket table definition, INSERT, ensure bucket number of new part is -1, ban recluster commands';
ALTER TABLE u10117_uniquekey_test_bucket2 DROP CLUSTER;
INSERT INTO u10117_uniquekey_test_bucket2 VALUES ('2023-06-25', 0, '0a');
SELECT * FROM u10117_uniquekey_test_bucket2 ORDER BY s, d;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket2' and active order by partition, bucket_number;
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION '2023-06-26';  -- { serverError 344 }
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION ID '20230626'; -- { serverError 344 }
ALTER TABLE u10117_uniquekey_test_bucket2 RECLUSTER PARTITION WHERE id > 0;  -- { serverError 344 }

ALTER TABLE u10117_uniquekey_test_bucket MODIFY SETTING partition_level_unique_keys = 1;
ALTER TABLE u10117_uniquekey_test_bucket MODIFY SETTING partition_level_unique_keys = 0; -- { serverError 344 }

DROP TABLE IF EXISTS u10117_uniquekey_test_bucket;
DROP TABLE IF EXISTS u10117_uniquekey_test_bucket2;
DROP TABLE IF EXISTS u10117_uniquekey_test_normal;

SELECT '';
select 'test bucket level unique key';
CREATE TABLE u10117_uniquekey_test_bucket (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY s INTO 2 BUCKETS ORDER BY s SETTINGS partition_level_unique_keys = 0;
CREATE TABLE u10117_uniquekey_test_bucket2 (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY s INTO 2 BUCKETS ORDER BY s SETTINGS partition_level_unique_keys = 1;

ALTER TABLE u10117_uniquekey_test_bucket MODIFY SETTING enable_bucket_level_unique_keys = 1;
ALTER TABLE u10117_uniquekey_test_bucket2 MODIFY SETTING enable_bucket_level_unique_keys = 1;
SELECT 'insert some values and query';
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-06-26', 3, '3a');
INSERT INTO u10117_uniquekey_test_bucket2 VALUES ('2023-06-26', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-06-26', 3, '3a'), ('2023-06-26', 1, '1b'), ('2023-06-26', 1, '1a'), ('2023-06-27', 1, '1c'), ('2023-06-27', 3, '3b');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket' and active;
SELECT * FROM u10117_uniquekey_test_bucket2 ORDER BY s, d;
SELECT partition, bucket_number, table_definition_hash FROM system.cnch_parts where database = currentDatabase(1) and table = 'u10117_uniquekey_test_bucket2' and active;

DROP TABLE IF EXISTS u10117_uniquekey_test_bucket;
DROP TABLE IF EXISTS u10117_uniquekey_test_bucket2;

set enable_wait_attached_staged_parts_to_visible = 0, enable_unique_table_attach_without_dedup = 0;
SELECT '';
SELECT 'test dedup correct staged parts in write process in table level';
CREATE TABLE u10117_uniquekey_test_bucket (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY s INTO 2 BUCKETS ORDER BY s SETTINGS partition_level_unique_keys = 0;

SELECT 'insert some values and query';
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-10-18', 2, '2a'), ('2023-06-26', 3, '3a'), ('2023-10-18', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-10-18', 3, '3a');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

SELECT 'stop dedup worker and let part into staging area, write process will handle all staged parts in table level lock';
SYSTEM STOP DEDUP WORKER u10117_uniquekey_test_bucket;
ALTER TABLE u10117_uniquekey_test_bucket DETACH PARTITION ID '20230626';
ALTER TABLE u10117_uniquekey_test_bucket DETACH PARTITION ID '20231018';
ALTER TABLE u10117_uniquekey_test_bucket ATTACH PARTITION ID '20230626';
ALTER TABLE u10117_uniquekey_test_bucket ATTACH PARTITION ID '20231018';
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 4, '4a');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

SELECT 'use bucket level lock and try again, write process will only handle bucket-related staged parts';
ALTER TABLE u10117_uniquekey_test_bucket MODIFY SETTING enable_bucket_level_unique_keys = 1;
SYSTEM STOP DEDUP WORKER u10117_uniquekey_test_bucket;
ALTER TABLE u10117_uniquekey_test_bucket DETACH PARTITION ID '20230626';
ALTER TABLE u10117_uniquekey_test_bucket DETACH PARTITION ID '20231018';
ALTER TABLE u10117_uniquekey_test_bucket ATTACH PARTITION ID '20230626';
ALTER TABLE u10117_uniquekey_test_bucket ATTACH PARTITION ID '20231018';
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 4, '4a');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

SELECT 'start dedup worker and all staged parts to be visible';
SYSTEM START DEDUP WORKER u10117_uniquekey_test_bucket;
SYSTEM SYNC DEDUP WORKER u10117_uniquekey_test_bucket;
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

DROP TABLE IF EXISTS u10117_uniquekey_test_bucket;

SELECT '';
SELECT 'test dedup correct staged parts in write process in partition level';
CREATE TABLE u10117_uniquekey_test_bucket (d Date, id Int32, s String) ENGINE = CnchMergeTree() PARTITION BY d UNIQUE KEY id CLUSTER BY s INTO 2 BUCKETS ORDER BY s SETTINGS partition_level_unique_keys = 1;

SELECT 'insert some values and query';
INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 1, '1a'), ('2023-10-18', 2, '2a'), ('2023-06-26', 3, '3a'), ('2023-10-18', 1, '1a'), ('2023-06-26', 2, '2a'), ('2023-10-18', 3, '3a');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

SELECT 'stop dedup worker and let part into staging area, write process will handle all staged parts in partition level lock';
SYSTEM STOP DEDUP WORKER u10117_uniquekey_test_bucket;
ALTER TABLE u10117_uniquekey_test_bucket DETACH PARTITION ID '20230626';
ALTER TABLE u10117_uniquekey_test_bucket DETACH PARTITION ID '20231018';
ALTER TABLE u10117_uniquekey_test_bucket ATTACH PARTITION ID '20230626';
ALTER TABLE u10117_uniquekey_test_bucket ATTACH PARTITION ID '20231018';
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 4, '4a');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;
SYSTEM START DEDUP WORKER u10117_uniquekey_test_bucket;
SYSTEM SYNC DEDUP WORKER u10117_uniquekey_test_bucket;

SELECT 'use bucket level lock and try again, write process will only handle bucket-related staged parts';
ALTER TABLE u10117_uniquekey_test_bucket MODIFY SETTING enable_bucket_level_unique_keys = 1;
SYSTEM STOP DEDUP WORKER u10117_uniquekey_test_bucket;
ALTER TABLE u10117_uniquekey_test_bucket DETACH PARTITION ID '20230626';
ALTER TABLE u10117_uniquekey_test_bucket DETACH PARTITION ID '20231018';
ALTER TABLE u10117_uniquekey_test_bucket ATTACH PARTITION ID '20230626';
ALTER TABLE u10117_uniquekey_test_bucket ATTACH PARTITION ID '20231018';
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

INSERT INTO u10117_uniquekey_test_bucket VALUES ('2023-06-26', 4, '4a');
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

SELECT 'start dedup worker and all staged parts to be visible';
SYSTEM START DEDUP WORKER u10117_uniquekey_test_bucket;
SYSTEM SYNC DEDUP WORKER u10117_uniquekey_test_bucket;
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY s, d;

DROP TABLE IF EXISTS u10117_uniquekey_test_bucket;

SELECT 'Test fetch right parts when enable_bucket_level_unique_keys=1';
CREATE TABLE u10117_uniquekey_test_bucket (d Int32, n Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY d UNIQUE KEY n CLUSTER BY EXPRESSION toUInt32(n%2) INTO 2 BUCKETS ORDER BY n SETTINGS partition_level_unique_keys = 1, enable_bucket_level_unique_keys = 1;
INSERT INTO u10117_uniquekey_test_bucket VALUES (0, 0, 0), (0, 1, 1), (1, 2, 2), (1, 3, 3);
INSERT INTO u10117_uniquekey_test_bucket SELECT number as d, number as n, number+10 as m FROM numbers(1);
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY n;

DROP TABLE IF EXISTS u10117_uniquekey_test_bucket;

SELECT 'Test fetch right parts when enable_bucket_level_unique_keys=0';
CREATE TABLE u10117_uniquekey_test_bucket (d Int32, n Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY d UNIQUE KEY n CLUSTER BY EXPRESSION toUInt32(m%2) INTO 2 BUCKETS ORDER BY n SETTINGS partition_level_unique_keys = 1, enable_bucket_level_unique_keys = 0;
INSERT INTO u10117_uniquekey_test_bucket VALUES (0, 0, 0), (0, 1, 1), (1, 2, 2), (1, 3, 3);
INSERT INTO u10117_uniquekey_test_bucket SELECT number as d, number as n, number+7 as m FROM numbers(1);
SELECT * FROM u10117_uniquekey_test_bucket ORDER BY n;

DROP TABLE IF EXISTS u10117_uniquekey_test_bucket;
