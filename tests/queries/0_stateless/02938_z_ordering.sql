
CREATE DATABASE IF NOT EXISTS test_02938;
USE test_02938;
CREATE TABLE wide_table (x UInt32, y UInt32, z UInt32) ENGINE = MergeTree ORDER BY mortonEncode(x, y, z) SETTINGS z_index_granularity = 3, index_granularity = 5;
SHOW CREATE TABLE wide_table;
CREATE TABLE wide_table2 (x UInt32, y UInt32, z UInt32) ENGINE = MergeTree ORDER BY mortonEncode(x, y, z) SETTINGS index_granularity = 5;
SHOW CREATE TABLE wide_table2;
CREATE TABLE wide_table3 (x UInt32, y UInt32, z UInt32) ENGINE = MergeTree ORDER BY mortonEncode(x, y, z) SETTINGS index_granularity = 5, enable_index_by_space_filling_curve = 0;
SHOW CREATE TABLE wide_table3;
DROP DATABASE test_02938;
