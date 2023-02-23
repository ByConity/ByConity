
CREATE DATABASE IF NOT EXISTS test_02938;
USE test_02938;
CREATE TABLE wide_table (x UInt32, y UInt32, z UInt32) ENGINE = MergeTree ORDER BY mortonEncode(x, y, z) SETTINGS z_index_granularity = 2, index_granularity = 5;
SHOW CREATE TABLE wide_table;
DROP DATABASE test_02938;
