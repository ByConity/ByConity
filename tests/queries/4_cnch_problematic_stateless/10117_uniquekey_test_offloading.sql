DROP TABLE IF EXISTS uniquekey_test_offloading;
DROP TABLE IF EXISTS uniquekey_test_offloading2;

CREATE TABLE uniquekey_test_offloading (d Date, id Int32, s String) ENGINE = CnchMergeTree()
PARTITION BY d ORDER BY s UNIQUE KEY id SETTINGS partition_level_unique_keys = 1;

-- Enable offloading mode
SET cnch_offloading_mode = 1;
SET enable_optimizer = 0;

INSERT INTO uniquekey_test_offloading VALUES ('2015-01-01', 10, 'a');
INSERT INTO uniquekey_test_offloading VALUES ('2015-01-01', 10, 'b');
INSERT INTO uniquekey_test_offloading VALUES ('2015-02-01', 30, 'a');
INSERT INTO uniquekey_test_offloading VALUES ('2015-02-01', 30, 'b');
INSERT INTO uniquekey_test_offloading VALUES ('2015-03-01', 50, 'e');

SELECT * FROM uniquekey_test_offloading ORDER BY id;

CREATE TABLE uniquekey_test_offloading2 (d Date, id Int32, s String) ENGINE = CnchMergeTree()
PARTITION BY d ORDER BY s UNIQUE KEY id SETTINGS partition_level_unique_keys = 1;

INSERT INTO uniquekey_test_offloading2 SELECT * FROM uniquekey_test_offloading;

SELECT * FROM uniquekey_test_offloading2 ORDER BY id;

DROP TABLE IF EXISTS uniquekey_test_offloading;
DROP TABLE IF EXISTS uniquekey_test_offloading2;
