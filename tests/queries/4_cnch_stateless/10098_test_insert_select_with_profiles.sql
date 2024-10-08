set enable_optimizer=0;

DROP TABLE IF EXISTS test_insert_all;
DROP TABLE IF EXISTS test_insert_all2;
CREATE TABLE test_insert_all (`id` UInt64, `name` String) ENGINE = CnchMergeTree() ORDER BY id SETTINGS index_granularity = 8192;

SET insert_select_with_profiles = 0;
SELECT 'insert_select_with_profiles = 0';
INSERT INTO test_insert_all SELECT number, toString(number) FROM system.numbers LIMIT 947;
INSERT INTO test_insert_all SELECT number, toString(number) FROM system.numbers LIMIT 52346;
INSERT INTO test_insert_all SELECT number, toString(number) FROM system.numbers LIMIT 2893572;
select sleep(2);
SELECT count() FROM test_insert_all;

CREATE TABLE test_insert_all2 (`id` UInt64, `name` String) ENGINE = CnchMergeTree() ORDER BY id SETTINGS index_granularity = 8192;
SET insert_select_with_profiles = 1;
SELECT 'insert_select_with_profiles = 1';
INSERT INTO test_insert_all2 SELECT number, toString(number) FROM system.numbers LIMIT 947;
INSERT INTO test_insert_all2 SELECT number, toString(number) FROM system.numbers LIMIT 52346;
INSERT INTO test_insert_all2 SELECT number, toString(number) FROM system.numbers LIMIT 2893572;
select sleep(2);
SELECT count() FROM test_insert_all2;

DROP TABLE IF EXISTS test_insert_all;
DROP TABLE IF EXISTS test_insert_all2;
