DROP DATABASE IF EXISTS test_partial;
CREATE DATABASE test_partial;
CREATE TABLE test_partial.cnch_partial_part_bucket(name String, age Int8) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 1 BUCKETS ORDER BY name;
INSERT INTO test_partial.cnch_partial_part_bucket VALUES ('kobe', 10);

SET cnch_part_allocation_algorithm = 3;
SELECT name, age FROM test_partial.cnch_partial_part_bucket;
ALTER TABLE test_partial.cnch_partial_part_bucket MODIFY COLUMN age UInt16;

SYSTEM START MERGES test_partial.cnch_partial_part_bucket;
SELECT sleep(3) FORMAT Null;
OPTIMIZE TABLE test_partial.cnch_partial_part_bucket;
SELECT sleep(3) FORMAT Null;
SELECT name, age FROM test_partial.cnch_partial_part_bucket;

DROP TABLE test_partial.cnch_partial_part_bucket;
DROP DATABASE test_partial;
