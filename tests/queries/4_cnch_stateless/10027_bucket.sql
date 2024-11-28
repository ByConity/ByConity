SET enable_multiple_tables_for_cnch_parts = 1;
DROP TABLE IF EXISTS bucket;
DROP TABLE IF EXISTS bucket2;
DROP TABLE IF EXISTS bucket3;
DROP TABLE IF EXISTS normal;
DROP TABLE IF EXISTS bucket_with_split_number;
DROP TABLE IF EXISTS bucket_with_split_number_n_range;
DROP TABLE IF EXISTS dts_bucket_with_split_number_n_range;
DROP TABLE IF EXISTS bucket_attach;
DROP TABLE IF EXISTS bucket_attach_2;
DROP TABLE IF EXISTS test_optimize;
DROP TABLE IF EXISTS test_optimize_with_date_column;
DROP TABLE IF EXISTS test_user_defined_expr;


CREATE TABLE bucket (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 1 BUCKETS ORDER BY name;
CREATE TABLE bucket2 (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 1 BUCKETS ORDER BY name;
CREATE TABLE bucket3 (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 1 BUCKETS ORDER BY name;
CREATE TABLE normal (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name ORDER BY name;
CREATE TABLE bucket_with_split_number (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY (name, age) INTO 1 BUCKETS SPLIT_NUMBER 60 ORDER BY name;
CREATE TABLE bucket_with_split_number_n_range (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY (name, age) INTO 1 BUCKETS SPLIT_NUMBER 60 WITH_RANGE ORDER BY name;
CREATE TABLE dts_bucket_with_split_number_n_range (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY (name) INTO 1 BUCKETS SPLIT_NUMBER 60 WITH_RANGE ORDER BY name;
CREATE TABLE bucket_attach (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 5 BUCKETS ORDER BY name;
CREATE TABLE bucket_attach_2 (name String, age Int64) ENGINE = CnchMergeTree() PARTITION BY name CLUSTER BY age INTO 2 BUCKETS ORDER BY name;
CREATE TABLE test_optimize (`id` UInt32, `code` UInt32, `record` String) ENGINE = CnchMergeTree PARTITION BY id CLUSTER BY code INTO 4 BUCKETS ORDER BY code;
CREATE TABLE test_optimize_with_date_column (`id` UInt32, `code` UInt32, `record` Date) ENGINE = CnchMergeTree PARTITION BY id CLUSTER BY record INTO 4 BUCKETS ORDER BY record;
CREATE TABLE test_user_defined_expr (`id` UInt32, `code` UInt32, `record` String) ENGINE = CnchMergeTree PARTITION BY id CLUSTER BY EXPRESSION empty(record) INTO 2 BUCKETS ORDER BY record;
CREATE TABLE test_user_defined_expr_error (`id` UInt32, `code` UInt32, `record` String) ENGINE = CnchMergeTree PARTITION BY id CLUSTER BY EXPRESSION record INTO 2 BUCKETS ORDER BY record; -- { serverError 36 }
CREATE TABLE test_user_defined_expr_error (`id` UInt32, `code` UInt32, `record` String) ENGINE = CnchMergeTree PARTITION BY id CLUSTER BY EXPRESSION (code*2, code) INTO 2 BUCKETS ORDER BY record; -- { serverError 36 }

-- Ensure bucket number is assigned to a part in bucket table
INSERT INTO bucket VALUES ('jane', 10);
SELECT * FROM bucket ORDER BY name FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = currentDatabase(1) and table = 'bucket' FORMAT CSV;

-- Ensure join queries between bucket tables work correctly
INSERT INTO bucket2 VALUES ('bob', 10);
SELECT * FROM bucket2 ORDER BY name FORMAT CSV;
SELECT b1.name, age, b2.name FROM bucket b1 JOIN bucket2 b2 USING (age) FORMAT CSV;

-- Attach part from bucket table to another bucket table of same table definition
ALTER TABLE bucket2 ATTACH PARTITION 'jane' from bucket;
SELECT partition FROM system.cnch_parts where database = currentDatabase(1) and table = 'bucket2' FORMAT CSV;

-- Attach part from bucket table to normal table
ALTER TABLE normal ATTACH PARTITION 'bob' from bucket2;
SELECT partition FROM system.cnch_parts where database = currentDatabase(1) and table = 'normal' FORMAT CSV;

-- ALTER bucket table definition and check that parts have different table_definition_hash due to lazy recluster
INSERT INTO bucket VALUES ('tracy', 20);
SELECT count(DISTINCT table_definition_hash) FROM system.cnch_parts where database = currentDatabase(1) and table = 'bucket' and active FORMAT CSV;
ALTER TABLE bucket MODIFY CLUSTER BY age INTO 3 BUCKETS;
INSERT INTO bucket VALUES ('jane', 10);
SELECT * FROM bucket ORDER BY name FORMAT CSV;
SELECT count(DISTINCT table_definition_hash) FROM system.cnch_parts where database = currentDatabase(1) and table = 'bucket' and active FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = currentDatabase(1) and table = 'bucket' and active FORMAT CSV;

-- DROP bucket table definition, INSERT, ensure new part's bucket number is -1
ALTER TABLE bucket3 DROP CLUSTER;
INSERT INTO bucket3 VALUES ('jack', 15);
SELECT * FROM bucket3 ORDER BY name FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = currentDatabase(1) and table = 'bucket3' FORMAT CSV;

-- Ensure bucket number is assigned to a part in bucket table with shard ratio 
INSERT INTO bucket_with_split_number VALUES ('vivek', 10);
SELECT * FROM bucket_with_split_number ORDER BY name FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = currentDatabase(1) and table = 'bucket_with_split_number' FORMAT CSV;
SELECT split_number, with_range FROM system.cnch_tables where database = currentDatabase(1) and name = 'bucket_with_split_number' FORMAT CSV;

-- Ensure bucket number is assigned to a part in bucket table with shard ratio and range
INSERT INTO bucket_with_split_number_n_range VALUES ('vivek', 20);
SELECT * FROM bucket_with_split_number_n_range ORDER BY name FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = currentDatabase(1) and table = 'bucket_with_split_number_n_range' FORMAT CSV;
SELECT split_number, with_range FROM system.cnch_tables where database = currentDatabase(1) and name = 'bucket_with_split_number_n_range' FORMAT CSV;

-- Ensure bucket number is assigned using DTSPartition with shard ratio and range
INSERT INTO dts_bucket_with_split_number_n_range VALUES ('vivek', 30);
SELECT * FROM dts_bucket_with_split_number_n_range ORDER BY name FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = currentDatabase(1) and table = 'dts_bucket_with_split_number_n_range' FORMAT CSV;
SELECT split_number, with_range FROM system.cnch_tables where database = currentDatabase(1) and name = 'dts_bucket_with_split_number_n_range' FORMAT CSV;

-- Attach partition is allowed between bucket tables with different table_definition_hash
INSERT INTO bucket_attach VALUES ('tracy', 20);
INSERT INTO bucket_attach_2 VALUES ('jane', 10);
SELECT count(DISTINCT table_definition_hash) FROM system.cnch_parts where database = currentDatabase(1) and table = 'bucket_attach' and active FORMAT CSV;
ALTER TABLE bucket_attach ATTACH PARTITION 'jane' from bucket_attach_2;
SELECT * FROM bucket_attach ORDER BY name FORMAT CSV;
SELECT * FROM bucket_attach_2 ORDER BY name FORMAT CSV; -- empty results returned as part has been dropped from this table during attach
SELECT count(DISTINCT table_definition_hash) FROM system.cnch_parts where database = currentDatabase(1) and table = 'bucket_attach' and active FORMAT CSV;
SELECT sleep(3) FORMAT Null; -- wait for cluster_status to be changed
SELECT cluster_status FROM system.cnch_table_info where database = currentDatabase(0) and table = 'bucket_attach' FORMAT CSV;

-- Ensure bucket number is assigned using user defined cluster by expression
INSERT INTO test_user_defined_expr VALUES (1, 1, 'r1'), (2, 2, '');
SELECT * FROM test_user_defined_expr ORDER BY id FORMAT CSV;
SELECT bucket_number FROM system.cnch_parts where database = currentDatabase(1) and table = 'test_user_defined_expr' ORDER BY bucket_number FORMAT CSV;
ALTER TABLE test_user_defined_expr MODIFY CLUSTER BY EXPRESSION code*2 INTO 2 BUCKETS;
TRUNCATE TABLE test_user_defined_expr;
INSERT INTO test_user_defined_expr VALUES (1, 0, 'r1');
SELECT bucket_number FROM system.cnch_parts where database = currentDatabase(1) and table = 'test_user_defined_expr' and active ORDER BY bucket_number FORMAT CSV;
INSERT INTO test_user_defined_expr VALUES (1, 1, 'r1'); -- { serverError 49 }
ALTER TABLE test_user_defined_expr RECLUSTER PARTITION WHERE 1; -- { serverError 344 }

-- Ensure optimize_skip_unused_workers 
INSERT INTO TABLE test_optimize select toUInt32(number/10), toUInt32(number/10), concat('record', toString(number)) from system.numbers limit 30;
SELECT * FROM  test_optimize where code = 2 ORDER BY record LIMIT 3 FORMAT CSV;
-- Apply optimization, note here will only check correctness. Integeration test framework should evaluate optimization in future.
SET optimize_skip_unused_shards = 1;
SELECT * FROM  test_optimize where code = 2 ORDER BY record LIMIT 3 FORMAT CSV;
-- Ensure that if we apply expression, the result is again same
SELECT * FROM test_optimize where code = toUInt32('2') ORDER BY record LIMIT 3 FORMAT CSV;
SELECT * FROM test_optimize where code in (0,2) ORDER BY record LIMIT 3 FORMAT CSV;
SET optimize_skip_unused_shards_limit = 1;
SELECT * FROM test_optimize where code in (0,2) ORDER BY record LIMIT 3 FORMAT CSV;
SET optimize_skip_unused_shards_limit = 1000;

-- Ensure other data type returns correct result with optimize_skip_unused_workers
INSERT INTO TABLE test_optimize_with_date_column SELECT toUInt32(number/10), (toUInt32(number/10)), toDate(number/10) FROM system.numbers LIMIT 30;
SET optimize_skip_unused_shards = 0;
SELECT * FROM test_optimize_with_date_column  WHERE record = '1970-01-02' ORDER BY id LIMIT 2 FORMAT CSV;
SET optimize_skip_unused_shards = 1;
SELECT * FROM test_optimize_with_date_column  WHERE record = '1970-01-02' ORDER BY id LIMIT 2 FORMAT CSV;

SET optimize_skip_unused_shards = 0;
SET enable_multiple_tables_for_cnch_parts = 0;
DROP TABLE bucket;
DROP TABLE bucket2;
DROP TABLE bucket3;
DROP TABLE normal;
DROP TABLE bucket_with_split_number;
DROP TABLE bucket_with_split_number_n_range;
DROP TABLE dts_bucket_with_split_number_n_range;
DROP TABLE bucket_attach;
DROP TABLE bucket_attach_2;
DROP TABLE test_optimize;
DROP TABLE test_optimize_with_date_column;
DROP TABLE test_user_defined_expr;

CREATE TABLE bucket (d UInt32, n UInt32) Engine = CnchMergeTree PARTITION BY d ORDER BY n;
INSERT INTO bucket VALUES (1, 1), (2, 2);
ALTER TABLE bucket MODIFY CLUSTER BY n INTO 2 BUCKETS;
SELECT cluster_status FROM system.cnch_table_info WHERE database = currentDatabase(0) AND table = 'bucket';
DROP TABLE bucket;

--- Check force_optimize_skip_unused_shards and multiple cluster by keys
SET optimize_skip_unused_shards = 1;
CREATE TABLE bucket (d UInt32, n UInt32) Engine = CnchMergeTree PARTITION BY d CLUSTER BY n INTO 2 BUCKETS ORDER BY n;
INSERT INTO bucket VALUES (1, 1), (2, 2);
SELECT * FROM bucket WHERE n = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 1;
SELECT * FROM bucket WHERE n = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 2;
SELECT * FROM bucket PREWHERE n = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 1;
SELECT * FROM bucket PREWHERE n = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 2;
SELECT * FROM bucket ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 1 ; -- { serverError 507 };
SELECT * FROM bucket ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 2 ; -- { serverError 507 };
DROP TABLE bucket;

CREATE TABLE bucket (d UInt32, n UInt32) Engine = CnchMergeTree PARTITION BY d ORDER BY n;
INSERT INTO bucket VALUES (1, 1), (2, 2);
SELECT * FROM bucket WHERE n = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 1;
SELECT * FROM bucket WHERE n = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 2; -- { serverError 507 };
SELECT * FROM bucket ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 1 ;
SELECT * FROM bucket ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 2 ; -- { serverError 507 };
DROP TABLE bucket;

CREATE TABLE bucket (d UInt32, n UInt32, v UInt32) Engine = CnchMergeTree PARTITION BY d CLUSTER BY (n,v) INTO 2 BUCKETS ORDER BY n;
INSERT INTO bucket VALUES (1, 1, 1), (2, 2, 2);
SELECT * FROM bucket WHERE n = 1 and v = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 1;
SELECT * FROM bucket WHERE n = 1 and v = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 2;
SELECT * FROM bucket PREWHERE n = 1 WHERE v = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 1;
SELECT * FROM bucket PREWHERE n = 1 WHERE v = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 2;
SELECT * FROM bucket WHERE n = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 1; -- { serverError 507 };
SELECT * FROM bucket WHERE n = 1 ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 2; -- { serverError 507 };
SELECT * FROM bucket ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 1 ; -- { serverError 507 };
SELECT * FROM bucket ORDER BY n FORMAT CSV SETTINGS force_optimize_skip_unused_shards = 2 ; -- { serverError 507 };
DROP TABLE bucket;
