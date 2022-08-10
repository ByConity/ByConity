DROP TABLE IF EXISTS test.uniquekey_test_offloading;
DROP TABLE IF EXISTS test.uniquekey_test_offloading2;

CREATE TABLE test.uniquekey_test_offloading (d Date, id Int32, s String) ENGINE = CnchMergeTree()
PARTITION BY d ORDER BY s UNIQUE KEY id SETTINGS partition_level_unique_keys = 1;

-- Enable offloading mode
SET cnch_offloading_mode = 1;
SET enable_optimizer = 0;

INSERT INTO test.uniquekey_test_offloading VALUES ('2015-01-01', 10, 'a');
INSERT INTO test.uniquekey_test_offloading VALUES ('2015-01-01', 10, 'b');
INSERT INTO test.uniquekey_test_offloading VALUES ('2015-02-01', 30, 'a');
INSERT INTO test.uniquekey_test_offloading VALUES ('2015-02-01', 30, 'b');
INSERT INTO test.uniquekey_test_offloading VALUES ('2015-03-01', 50, 'e');

SELECT * FROM test.uniquekey_test_offloading ORDER BY id;

CREATE TABLE test.uniquekey_test_offloading2 (d Date, id Int32, s String) ENGINE = CnchMergeTree()
PARTITION BY d ORDER BY s UNIQUE KEY id SETTINGS partition_level_unique_keys = 1;

INSERT INTO test.uniquekey_test_offloading2 SELECT * FROM test.uniquekey_test_offloading;

SELECT * FROM test.uniquekey_test_offloading2 ORDER BY id;

DROP TABLE IF EXISTS test.uniquekey_test_offloading;
DROP TABLE IF EXISTS test.uniquekey_test_offloading2;
