SET optimize_map_column_serialization = 1;

-- value type will automatically adding nullable
DROP TABLE IF EXISTS 10099_test_optimize_map_column_serialization;
CREATE TABLE 10099_test_optimize_map_column_serialization (s String, m Map(String, Int64)) engine = CnchMergeTree order by s settings index_granularity = 2;
INSERT INTO 10099_test_optimize_map_column_serialization VALUES ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2});
SELECT m{'k1'}, m{'k2'} FROM 10099_test_optimize_map_column_serialization;

-- value type low cardinality
DROP TABLE IF EXISTS 10099_test_optimize_map_column_serialization;
CREATE TABLE 10099_test_optimize_map_column_serialization (s String, m Map(String, LowCardinality(Nullable(Int64)))) engine = CnchMergeTree order by s settings index_granularity = 2;
INSERT INTO 10099_test_optimize_map_column_serialization VALUES ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2});
SELECT m{'k1'}, m{'k2'} FROM 10099_test_optimize_map_column_serialization;

SET optimize_map_column_serialization = 0;

-- value type will automatically adding nullable
DROP TABLE IF EXISTS 10099_test_optimize_map_column_serialization;
CREATE TABLE 10099_test_optimize_map_column_serialization (s String, m Map(String, Int64)) engine = CnchMergeTree order by s settings index_granularity = 2;
INSERT INTO 10099_test_optimize_map_column_serialization VALUES ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2});
SELECT m{'k1'}, m{'k2'} FROM 10099_test_optimize_map_column_serialization;

-- value type low cardinality
DROP TABLE IF EXISTS 10099_test_optimize_map_column_serialization;
CREATE TABLE 10099_test_optimize_map_column_serialization (s String, m Map(String, LowCardinality(Nullable(Int64)))) engine = CnchMergeTree order by s settings index_granularity = 2;
INSERT INTO 10099_test_optimize_map_column_serialization VALUES ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2});
SELECT m{'k1'}, m{'k2'} FROM 10099_test_optimize_map_column_serialization;

DROP TABLE IF EXISTS 10099_test_optimize_map_column_serialization;
