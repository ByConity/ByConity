DROP TABLE IF EXISTS test_ingest_partition_target;
DROP TABLE IF EXISTS test_ingest_partition_source;

CREATE TABLE test_ingest_partition_target (p_date Date, id1 Int32, id2 Int32, c1 String, c2 String, c3 String) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY (id1, id2) SETTINGS index_granularity = 8192;
CREATE TABLE test_ingest_partition_source (p_date Date, id1 Int32, id2 Int32, c1 String, c2 String) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY (id1, id2) SETTINGS index_granularity = 8192;

SYSTEM START MERGES test_ingest_partition_target;

-- empty target data
INSERT INTO test_ingest_partition_source VALUES ('2010-01-01', 1, 2, 'a', 'b'), ('2010-01-01', 3, 4, 'a', 'b'), ('2010-01-01', 5, 6, 'a', 'b'), ('2010-01-01', 7, 8, 'a', 'b');

ALTER TABLE test_ingest_partition_target ingest partition '2010-01-01' columns c1, c2 key id1, id2 from test_ingest_partition_source SETTINGS memory_efficient_ingest_partition_max_key_count_in_memory = 2;

SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;

-- empty source data
TRUNCATE TABLE test_ingest_partition_source;
ALTER TABLE test_ingest_partition_target ingest partition '2010-01-01' columns c1, c2 key id1, id2 from test_ingest_partition_source SETTINGS memory_efficient_ingest_partition_max_key_count_in_memory = 2;
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;

SELECT 'SOURCE and TARGET have same key, same row count: START';
INSERT INTO test_ingest_partition_source VALUES ('2010-01-01', 1, 2, 'c', 'd'), ('2010-01-01', 3, 4, 'c', 'd'), ('2010-01-01', 5, 6, 'c', 'd'), ('2010-01-01', 7, 8, 'c', 'd');

SELECT 'SOURCE';
SELECT * FROM test_ingest_partition_source ORDER BY id1, id2;
SELECT 'TARGET';
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
ALTER TABLE test_ingest_partition_target ingest partition '2010-01-01' columns c1, c2 key id1, id2 from test_ingest_partition_source SETTINGS memory_efficient_ingest_partition_max_key_count_in_memory = 2;
SELECT 'RESULT';
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
SELECT 'SOURCE and TARGET have same key, same row count: END';

SELECT 'SOURCE has key that TARGET dont have, TARGET has key that SOURCE dont have: START';
TRUNCATE TABLE test_ingest_partition_source;
INSERT INTO test_ingest_partition_source VALUES ('2010-01-01', 1, 2, 'd', 'e'), ('2010-01-01', 3, 4, 'd', 'e'), ('2010-01-01', 5, 6, 'd', 'e'), ('2010-01-01', 7, 8, 'd', 'e'), ('2010-01-01', 2, 3, 'e', 'f');
SELECT 'SOURCE';
SELECT * FROM test_ingest_partition_source ORDER BY id1, id2;
SELECT 'TARGET';
INSERT INTO test_ingest_partition_target VALUES ('2010-01-01', 2, 2, 'd', 'e', '');
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_target ingest partition '2010-01-01' columns c1, c2 key id1, id2 from test_ingest_partition_source SETTINGS memory_efficient_ingest_partition_max_key_count_in_memory = 2, memory_efficient_ingest_partition_max_key_count_in_memory = 2;
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
SELECT 'SOURCE has key that TARGET dont have, TARGET has key that SOURCE dont have: END';

DROP TABLE IF EXISTS test_ingest_partition_target;
SELECT 'SOURCE has key that TARGET dont have, TARGET has key that SOURCE dont have, ingest_default_column_value_if_not_provided=false: START';
CREATE TABLE test_ingest_partition_target (p_date Date, id1 Int32, id2 Int32, c1 String, c2 String, c3 String) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY (id1, id2) SETTINGS ingest_default_column_value_if_not_provided = 0;
SYSTEM START MERGES test_ingest_partition_target;
INSERT INTO test_ingest_partition_target VALUES ('2010-01-01', 2, 2, 'd', 'e', ''), ('2010-01-01', 1, 2, 'c', 'd', ''), ('2010-01-01', 3, 4, 'c', 'd', ''), ('2010-01-01', 5, 6, 'c', 'd', ''), ('2010-01-01', 7, 8, 'c', 'd', '');
SELECT 'SOURCE';
SELECT * FROM test_ingest_partition_source ORDER BY id1, id2;
SELECT 'TARGET';
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_target ingest partition '2010-01-01' columns c1, c2 key id1, id2 from test_ingest_partition_source SETTINGS memory_efficient_ingest_partition_max_key_count_in_memory = 2;
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
SELECT 'SOURCE has key that TARGET dont have, TARGET has key that SOURCE dont have, ingest_default_column_value_if_not_provided=false: END';

SELECT 'SOURCE and TARGET dont share key, ingest_default_column_value_if_not_provided=false: START';
TRUNCATE TABLE test_ingest_partition_source;
INSERT INTO test_ingest_partition_source VALUES ('2010-01-01', 1, 1, 'a', 'b'), ('2010-01-01', 3, 3, 'a', 'b'), ('2010-01-01', 5, 5, 'a', 'b'), ('2010-01-01', 7, 7, 'a', 'b');
SELECT 'SOURCE';
SELECT * FROM test_ingest_partition_source ORDER BY id1, id2;
SELECT 'TARGET';
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
ALTER TABLE test_ingest_partition_target ingest partition '2010-01-01' columns c1, c2 key id1, id2 from test_ingest_partition_source SETTINGS memory_efficient_ingest_partition_max_key_count_in_memory = 2;
SELECT 'RESULT';
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
SELECT 'SOURCE and TARGET dont share key, ingest_default_column_value_if_not_provided=false: END';
DROP TABLE IF EXISTS test_ingest_partition_target;
CREATE TABLE test_ingest_partition_target (p_date Date, id1 Int32, id2 Int32, c1 String, c2 String, c3 String) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY (id1, id2) SETTINGS index_granularity = 8192;
SYSTEM START MERGES test_ingest_partition_target;
SELECT 'SOURCE and TARGET dont share key: START';
SELECT 'SOURCE';
SELECT * FROM test_ingest_partition_source ORDER BY id1, id2;
SELECT 'TARGET';
INSERT INTO test_ingest_partition_target VALUES ('2010-01-01', 2, 2, 'a', 'b', ''), ('2010-01-01', 4, 4, 'a', 'b', ''), ('2010-01-01', 6, 6, 'a', 'b', ''),('2010-01-01', 8, 8, 'a', 'b', ''),;
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_target ingest partition '2010-01-01' columns c1, c2 key id1, id2 from test_ingest_partition_source SETTINGS memory_efficient_ingest_partition_max_key_count_in_memory = 2;
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
SELECT 'SOURCE and TARGET dont share key: END';
SELECT '1 part in TARGET share key with 2 parts in SOURCE: START';
TRUNCATE TABLE test_ingest_partition_source;
TRUNCATE TABLE test_ingest_partition_target;
INSERT INTO test_ingest_partition_target VALUES ('2010-01-01', 2, 2, 'a', 'b', 'c'), ('2010-01-01', 4, 4, 'a', 'b', 'c'), ('2010-01-01', 6, 6, 'a', 'b', 'c'),('2010-01-01', 8, 8, 'a', 'b', 'c'),;
INSERT INTO test_ingest_partition_source VALUES ('2010-01-01', 2, 2, 'e', 'f'), ('2010-01-01', 3, 3, 'e', 'f'), ('2010-01-01', 4, 4, 'e', 'f');
INSERT INTO test_ingest_partition_source VALUES ('2010-01-01', 5, 5, 'e', 'f'), ('2010-01-01', 6, 6, 'e', 'f'), ('2010-01-01', 7, 7, 'e', 'f');
SELECT 'SOURCE';
SELECT * FROM test_ingest_partition_source ORDER BY id1, id2;
SELECT 'TARGET';
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
SELECT 'RESULT';
ALTER TABLE test_ingest_partition_target ingest partition '2010-01-01' columns c1, c2 key id1, id2 from test_ingest_partition_source SETTINGS memory_efficient_ingest_partition_max_key_count_in_memory = 2;
SELECT * FROM test_ingest_partition_target ORDER BY id1, id2;
SELECT '1 part in TARGET share key with 2 parts in SOURCE: END';
DROP TABLE IF EXISTS test_ingest_partition_target;
DROP TABLE IF EXISTS test_ingest_partition_source;

