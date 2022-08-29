USE test;

DROP TABLE IF EXISTS test.bucket;
DROP TABLE IF EXISTS test.bucket2;
DROP TABLE IF EXISTS test.bucket3;
DROP TABLE IF EXISTS test.bucket_with_split_number;
DROP TABLE IF EXISTS test.bucket_with_split_number_n_range;
DROP TABLE IF EXISTS test.dts_bucket_with_split_number_n_range;


CREATE TABLE test.bucket (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 1 BUCKETS ORDER BY name;
CREATE TABLE test.bucket2 (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 1 BUCKETS ORDER BY name;
CREATE TABLE test.bucket3 (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 1 BUCKETS ORDER BY name;
CREATE TABLE test.bucket_with_split_number (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY (name, age) INTO 1 BUCKETS SPLIT_NUMBER 60 ORDER BY name;
CREATE TABLE test.bucket_with_split_number_n_range (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY (name, age) INTO 1 BUCKETS SPLIT_NUMBER 60 WITH_RANGE ORDER BY name;
CREATE TABLE test.dts_bucket_with_split_number_n_range (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY (name) INTO 1 BUCKETS SPLIT_NUMBER 60 WITH_RANGE ORDER BY name;

-- Ensure bucket number is assigned to a part in bucket table
INSERT INTO test.bucket VALUES ('jane', 10);
SELECT * FROM test.bucket ORDER BY name FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = 'test' and table = 'bucket' FORMAT CSV;

-- Ensure join queries between bucket tables work correctly
INSERT INTO test.bucket2 VALUES ('bob', 10);
SELECT * FROM test.bucket2 ORDER BY name FORMAT CSV;
SELECT b1.name, age, b2.name FROM test.bucket b1 JOIN test.bucket2 b2 USING (age) FORMAT CSV;

ALTER TABLE test.bucket MODIFY CLUSTER BY age INTO 3 BUCKETS;
-- TODO: to uncomment after clustering task. Expected bucket number value is 1
-- SELECT * FROM test.bucket ORDER BY name FORMAT CSV;
-- SELECT bucket_number FROM system.cnch_parts where database = 'test' and table = 'bucket' FORMAT CSV;

-- TODO: to uncomment once system tables are fixed
-- DROP bucket table definition, INSERT, ensure new part's bucket number is -1
ALTER TABLE test.bucket3 DROP CLUSTER;
-- INSERT INTO test.bucket3 VALUES ('jack', 15);
-- SELECT * FROM test.bucket3 ORDER BY name FORMAT CSV;
-- SELECT bucket_number FROM system.cnch_parts where database = 'test' and table = 'bucket3' FORMAT CSV;


-- Ensure bucket number is assigned to a part in bucket table with shard ratio 
INSERT INTO test.bucket_with_split_number VALUES ('vivek', 10);
SELECT * FROM test.bucket_with_split_number ORDER BY name FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = 'test' and table = 'bucket_with_split_number' FORMAT CSV;
-- SELECT split_number, with_range FROM system.cnch_tables where database = 'test' and name = 'bucket_with_split_number' FORMAT CSV; 60,0

-- Ensure bucket number is assigned to a part in bucket table with shard ratio and range
INSERT INTO test.bucket_with_split_number_n_range VALUES ('vivek', 20);
SELECT * FROM test.bucket_with_split_number_n_range ORDER BY name FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = 'test' and table = 'bucket_with_split_number_n_range' FORMAT CSV;
-- SELECT split_number, with_range FROM system.cnch_tables where database = 'test' and name = 'bucket_with_split_number_n_range' FORMAT CSV; 60,1

-- Ensure bucket number is assigned using DTSPartition with shard ratio and range
INSERT INTO test.dts_bucket_with_split_number_n_range VALUES ('vivek', 30);
SELECT * FROM test.dts_bucket_with_split_number_n_range ORDER BY name FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = 'test' and table = 'dts_bucket_with_split_number_n_range' FORMAT CSV;
-- SELECT split_number, with_range FROM system.cnch_tables where database = 'test' and name = 'dts_bucket_with_split_number_n_range' FORMAT CSV; 60,1


DROP TABLE test.bucket;
DROP TABLE test.bucket2;
DROP TABLE test.bucket3;
DROP TABLE test.bucket_with_split_number;
DROP TABLE test.bucket_with_split_number_n_range;
DROP TABLE test.dts_bucket_with_split_number_n_range;
