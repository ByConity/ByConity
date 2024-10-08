DROP TABLE IF EXISTS test_ingest_partition_map2_target;
DROP TABLE IF EXISTS test_ingest_partition_map2_source;

CREATE TABLE test_ingest_partition_map2_target (`date` Date, `id` Int32, `name` Map(String, String)) ENGINE = CnchMergeTree PARTITION BY date ORDER BY id; 
CREATE TABLE test_ingest_partition_map2_source (`date` Date, `id` Int32, `name` Map(String, String)) ENGINE = CnchMergeTree PARTITION BY date ORDER BY id; 

SYSTEM STOP MERGES test_ingest_partition_map2_source;
SYSTEM START MERGES test_ingest_partition_map2_target;
SELECT 'Source data have 2 parts unsorted join to 1 target data part: START';
INSERT INTO test_ingest_partition_map2_source VALUES ('2020-01-01', 1, {'key1': 's1', 'key2': 's2', 'key3': 's3'}), ('2020-01-01', 4, {'key1': 's1', 'key2': 's2', 'key3': 's3'});
INSERT INTO test_ingest_partition_map2_source VALUES ('2020-01-01', 2, {'key1': 's1', 'key2': 's2', 'key3': 's3'}), ('2020-01-01', 3, {'key1': 's1', 'key2': 's2', 'key3': 's3'});
SELECT 'SOURCE';
SELECT * from test_ingest_partition_map2_source ORDER BY id;
SELECT 'TARGET';
INSERT INTO test_ingest_partition_map2_target VALUES ('2020-01-01', 1, {'key1': 't1', 'key2': 't2', 'key3': 't3'}), ('2020-01-01', 2, {'key1': 't1', 'key2': 't2', 'key3': 't3'}), ('2020-01-01', 3, {'key1': 't1', 'key2': 't2', 'key3': 't3'}), ('2020-01-01', 4, {'key1': 't1', 'key2': 't2', 'key3': 't3'});
SELECT * from test_ingest_partition_map2_target ORDER BY id;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_map2_target INGEST PARTITION '2020-01-01'  COLUMNS name{'key1'}, name{'key2'} FROM test_ingest_partition_map2_source;
SELECT * FROM test_ingest_partition_map2_target ORDER BY id;
SELECT 'Source data have 2 parts unsorted join to 1 target data part: END';
DROP TABLE IF EXISTS test_ingest_partition_map2_target;
DROP TABLE IF EXISTS test_ingest_partition_map2_source;

