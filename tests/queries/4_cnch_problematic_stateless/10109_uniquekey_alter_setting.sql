DROP TABLE IF EXISTS test.u10109_pl;
DROP TABLE IF EXISTS test.u10109_tl;
CREATE TABLE test.u10109_pl (d Date, k1 Int64, v1 Int32) ENGINE=CnchMergeTree() PARTITION BY d ORDER BY k1 UNIQUE KEY k1 SETTINGS partition_level_unique_keys = 1;
CREATE TABLE test.u10109_tl (d Date, k1 Int64, v1 Int32) ENGINE=CnchMergeTree() PARTITION BY d ORDER BY k1 UNIQUE KEY k1 SETTINGS partition_level_unique_keys = 0;

-- modify partition_level_unique_keys is not allowed
ALTER TABLE test.u10109_pl MODIFY SETTING partition_level_unique_keys = 0; -- { serverError 344 }
ALTER TABLE test.u10109_tl MODIFY SETTING partition_level_unique_keys = 1; -- { serverError 344 }

-- enable memory buffer is not allowed
ALTER TABLE test.u10109_pl MODIFY SETTING cnch_enable_memory_buffer = 1; -- { serverError 344 }
-- disable memory buffer is allowed
ALTER TABLE test.u10109_pl MODIFY SETTING cnch_enable_memory_buffer = 0;

DROP TABLE IF EXISTS test.u10109_pl;
DROP TABLE IF EXISTS test.u10109_tl;
