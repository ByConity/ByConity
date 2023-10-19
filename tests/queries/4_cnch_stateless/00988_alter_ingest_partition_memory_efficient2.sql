DROP TABLE IF EXISTS test_ingest_partition_me2_target;
DROP TABLE IF EXISTS test_ingest_partition_me2_source;

CREATE TABLE test_ingest_partition_me2_target (p_date Date, id1 Int32, id2 Int32, c1 String, c2 String, c3 String) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY (id1, id2) SETTINGS index_granularity = 8192;
CREATE TABLE test_ingest_partition_me2_source (p_date Date, id1 Int32, id2 Int32, c1 String, c2 String) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY (id1, id2) SETTINGS index_granularity = 8192;

SYSTEM STOP MERGES test_ingest_partition_me2_source;
SYSTEM START MERGES test_ingest_partition_me2_target;
SELECT 'Source data have 2 parts unsorted join to 1 target data part: START';

INSERT INTO test_ingest_partition_me2_source VALUES ('2010-01-01', 1, 2, 'a', 'b'), ('2010-01-01', 3, 4, 'a', 'b');
INSERT INTO test_ingest_partition_me2_source VALUES ('2010-01-01', 2, 2, 'a', 'b'), ('2010-01-01', 2, 3, 'a', 'b');

SELECT 'SOURCE';
SELECT * FROM test_ingest_partition_me2_source ORDER BY id1, id2;

INSERT INTO test_ingest_partition_me2_target VALUES ('2010-01-01', 1, 2, 'c', 'd', 'e'), ('2010-01-01', 3, 4, 'c', 'd', 'e'), ('2010-01-01', 2, 2, 'c', 'd', 'e'), ('2010-01-01', 2, 3, 'c', 'd', 'e');
SELECT 'TARGET';
SELECT * FROM test_ingest_partition_me2_target ORDER BY id1, id2;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_me2_target ingest partition '2010-01-01' columns c1, c2 key id1, id2 from test_ingest_partition_me2_source Settings memory_efficient_ingest_partition_max_key_count_in_memory = 2;

SELECT * FROM test_ingest_partition_me2_target ORDER BY id1, id2;
SELECT 'Source data have 2 parts unsorted join to 1 target data part: END';

DROP TABLE IF EXISTS test_ingest_partition_me2_target;
DROP TABLE IF EXISTS test_ingest_partition_me2_source;
