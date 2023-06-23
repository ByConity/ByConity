DROP TABLE IF EXISTS test.test_ingest_partition_target;
DROP TABLE IF EXISTS test.test_ingest_partition_source;

CREATE TABLE test.test_ingest_partition_target (p_date Date, id1 Int32, id2 Int32, c1 String, c2 String, c3 String) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY (id1, id2) SETTINGS index_granularity = 8192;
CREATE TABLE test.test_ingest_partition_source (p_date Date, id1 Int32, id2 Int32, c1 String, c2 String) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY (id1, id2) SETTINGS index_granularity = 8192;

SYSTEM STOP MERGES test.test_ingest_partition_source;
SYSTEM START MERGES test.test_ingest_partition_target;
SELECT 'Source data have 2 parts unsorted join to 1 target data part: START';

INSERT INTO test.test_ingest_partition_source VALUES ('2010-01-01', 1, 2, 'a', 'b'), ('2010-01-01', 3, 4, 'a', 'b');
INSERT INTO test.test_ingest_partition_source VALUES ('2010-01-01', 2, 2, 'a', 'b'), ('2010-01-01', 2, 3, 'a', 'b');

SELECT 'SOURCE';
SELECT * FROM test.test_ingest_partition_source ORDER BY id1, id2;

INSERT INTO test.test_ingest_partition_target VALUES ('2010-01-01', 1, 2, 'c', 'd', 'e'), ('2010-01-01', 3, 4, 'c', 'd', 'e'), ('2010-01-01', 2, 2, 'c', 'd', 'e'), ('2010-01-01', 2, 3, 'c', 'd', 'e');
SELECT 'TARGET';
SELECT * FROM test.test_ingest_partition_target ORDER BY id1, id2;
SELECT 'RESULT';
ALTER TABLE test.test_ingest_partition_target ingest partition '2010-01-01' columns c1, c2 key id1, id2 from test.test_ingest_partition_source Settings memory_efficient_ingest_partition_max_key_count_in_memory = 2;

SELECT * FROM test.test_ingest_partition_target ORDER BY id1, id2;
SELECT 'Source data have 2 parts unsorted join to 1 target data part: END';

DROP TABLE IF EXISTS test.test_ingest_partition_target;
DROP TABLE IF EXISTS test.test_ingest_partition_source;
