USE test;

DROP TABLE IF EXISTS test.bucket;

CREATE TABLE test.bucket (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 1 BUCKETS ORDER BY name;

DROP TABLE test.bucket;
