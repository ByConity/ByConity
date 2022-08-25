USE test;

DROP TABLE IF EXISTS test.bucket;
DROP TABLE IF EXISTS test.bucket2;

CREATE TABLE test.bucket (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 1 BUCKETS ORDER BY name;
CREATE TABLE test.bucket2 (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 1 BUCKETS ORDER BY name;

-- Ensure bucket number is assigned to a part in bucket table
INSERT INTO test.bucket VALUES ('jane', 10);
SELECT * FROM test.bucket ORDER BY name FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = 'test' and table = 'bucket' FORMAT CSV;

-- Ensure join queries between bucket tables work correctly
INSERT INTO test.bucket2 VALUES ('bob', 10);
SELECT * FROM test.bucket2 ORDER BY name FORMAT CSV;
SELECT b1.name, age, b2.name FROM test.bucket b1 JOIN test.bucket2 b2 USING (age) FORMAT CSV;

ALTER TABLE test.bucket MODIFY CLUSTER BY age INTO 3 BUCKETS;
-- to uncomment after clustering task. Expected bucket number value is 1
-- SELECT * FROM test.bucket ORDER BY name FORMAT CSV;
-- SELECT bucket_number FROM system.cnch_parts where database = 'test' and table = 'bucket' FORMAT CSV;

DROP TABLE test.bucket;
DROP TABLE test.bucket2;
